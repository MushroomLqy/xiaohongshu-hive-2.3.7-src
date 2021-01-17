/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

import com.google.common.collect.ImmutableList;

import java.util.*;

public class HiveProjectSortTransposeRule extends RelOptRule {

  public static final HiveProjectSortTransposeRule INSTANCE =
      new HiveProjectSortTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HiveProjectSortTransposeRule.
   */
  private HiveProjectSortTransposeRule() {
    super(
        operand(
            HiveProject.class,
            operand(HiveSortLimit.class, any())));
  }

  protected HiveProjectSortTransposeRule(RelOptRuleOperand operand) {
    super(operand);
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch2(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);
    final HiveSortLimit sort = call.rel(1);

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutation(
            project.getProjects(), project.getInput().getRowType()).inverse();
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTarget(fc.getFieldIndex()) < 0) {
        return;
      }
    }

    // Create new collation
    final RelCollation newCollation =
        RelCollationTraitDef.INSTANCE.canonize(
            RexUtil.apply(map, sort.getCollation()));

    // New operators
    final RelNode newProject = project.copy(sort.getInput().getTraitSet(),
            ImmutableList.<RelNode>of(sort.getInput()));
    final HiveSortLimit newSort = sort.copy(newProject.getTraitSet(),
            newProject, newCollation, sort.offset, sort.fetch);

    call.transformTo(newSort);
  }

  public void onMatch(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);
    final HiveSortLimit sort = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    // Determine mapping between project input and output fields.
    // In Hive, Sort is always based on RexInputRef
    // We only need to check if project can contain all the positions that sort needs.
//    final Mappings.TargetMapping map =
//            RelOptUtil.permutationIgnoreCast(
//                    project.getProjects(), project.getInput().getRowType()).inverse();
    final Mappings.TargetMapping map =
            RelOptUtil.permutation(
                    project.getProjects(), project.getInput().getRowType()).inverse();
    Set<Integer> needed = new HashSet<>();
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      needed.add(fc.getFieldIndex());
      final RexNode node = project.getProjects().get(map.getTarget(fc.getFieldIndex()));
      if (node.isA(SqlKind.CAST)) {
        // Check whether it is a monotonic preserving cast, otherwise we cannot push
        final RexCall cast = (RexCall) node;
        final RexCallBinding binding =
                RexCallBinding.create(cluster.getTypeFactory(), cast,
                        ImmutableList.of(RexUtil.apply(map, sort.getCollation())));
        if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
          return;
        }
      }
    }
    Map<Integer,Integer> m = new HashMap<>();
    for (int projPos = 0; projPos < project.getChildExps().size(); projPos++) {
      RexNode expr = project.getChildExps().get(projPos);
      if (expr instanceof RexInputRef) {
        Set<Integer> positions = HiveCalciteUtil.getInputRefs(expr);
        if (positions.size() > 1) {
          continue;
        } else {
          int parentPos = positions.iterator().next();
          if(needed.contains(parentPos)){
            m.put(parentPos, projPos);
            needed.remove(parentPos);
          }
        }
      }
    }
    if(!needed.isEmpty()){
      return;
    }

    List<RelFieldCollation> fieldCollations = new ArrayList<>();
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      fieldCollations.add(new RelFieldCollation(m.get(fc.getFieldIndex()), fc.direction,
              fc.nullDirection));
    }

    RelTraitSet traitSet = sort.getCluster().traitSetOf(HiveRelNode.CONVENTION);
    RelCollation newCollation = traitSet.canonize(RelCollationImpl.of(fieldCollations));

    // New operators
    final RelNode newProject = project.copy(sort.getInput().getTraitSet(),
            ImmutableList.<RelNode>of(sort.getInput()));
    final HiveSortLimit newSort = sort.copy(newProject.getTraitSet(),
            newProject, newCollation, sort.offset, sort.fetch);

    call.transformTo(newSort);
  }

}

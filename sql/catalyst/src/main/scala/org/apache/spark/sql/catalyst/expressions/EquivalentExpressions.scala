/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable
import org.apache.spark.sql.internal.SQLConf

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions {
  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    addExprToMap(expr, equivalenceMap)
  }

  private def addExprToMap(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats],
      conditional: Boolean = false): Boolean = {
    if (expr.deterministic) {
      val wrapper = ExpressionEquals(expr)
      map.get(wrapper) match {
        case Some(stats) =>
          if (conditional) {
            stats.conditionalUseCount += 1
          } else {
            stats.useCount += 1
          }
          true
        case _ =>
          val stats = if (conditional) {
            ExpressionStats(expr)(0, 1)
          } else {
            ExpressionStats(expr)()
          }
          map.put(wrapper, stats)
          false
      }
    } else {
      false
    }
  }

  /**
   * Finds only expressions which are common in each of given expressions, in a recursive way.
   * For example, given two expressions `(a + (b + (c + 1)))` and `(d + (e + (c + 1)))`,
   * the common expression `(c + 1)` will be returned.
   *
   * Note that as we don't know in advance if any child node of an expression will be common
   * across all given expressions, we count all child nodes when looking through the given
   * expressions. But when we call `addExprTree` to add common expressions into the map, we
   * will add recursively the child nodes. So we need to filter the child expressions first.
   * For example, if `((a + b) + c)` and `(a + b)` are common expressions, we only add
   * `((a + b) + c)`.
   */
  private def findCommonExprs(exprs: Seq[Expression]): Seq[ExpressionEquals] = {
    assert(exprs.length > 1)
    var localEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
    addExprTree(exprs.head, localEquivalenceMap)

    exprs.tail.foreach { expr =>
      val otherLocalEquivalenceMap = mutable.HashMap.empty[ExpressionEquals, ExpressionStats]
      addExprTree(expr, otherLocalEquivalenceMap)
      localEquivalenceMap = localEquivalenceMap.filter { case (key, _) =>
        otherLocalEquivalenceMap.contains(key)
      }
    }

    localEquivalenceMap.filter { case (commonExpr, state) =>
      // If the `commonExpr` already appears in the equivalence map, calling `addExprTree` will
      // increase the `useCount` and mark it as a common subexpression. Otherwise, `addExprTree`
      // will recursively add `commonExpr` and its descendant to the equivalence map, in case
      // they also appear in other places. For example, `If(a + b > 1, a + b + c, a + b + c)`,
      // `a + b` also appears in the condition and should be treated as common subexpression.
      val possibleParents = localEquivalenceMap.filter { case (_, v) => v.height > state.height }
      possibleParents.forall { case (k, _) =>
        k == commonExpr || k.e.find(_.semanticEquals(commonExpr.e)).isEmpty
      }
    }.map(_._1).toSeq
  }

  /**
   * There are some expressions that need special handling:
   *    1. CodegenFallback: It's children will not be used to generate code (call eval() instead).
   *    2. If: Only the predicate will be always evaluated. If there are common
   *           expressions among the true and false values, those will always be evaluated as well.
   *           Otherwise they are conditionally evaluated.
   *    3. CaseWhen: Like `If`, only the first predicate will always be evaluated. The remaining
   *                 predicates will only be conditionally evaluated. If there are common
   *                 expressions among the values, those will always be evaluated, otherwise they
   *                 are conditionally evaluated.
   *    4. Coalesce: Only the first expressions will always be evaluated, the rest will be
   *                 conditionally evaluated.
   */
  private def childrenToRecurse(expr: Expression): RecurseChildren = expr match {
    case _: CodegenFallback => RecurseChildren(Nil)
    case i: If =>
      val values = Seq(i.trueValue, i.falseValue)
      RecurseChildren(Seq(i.predicate), values, values)
    case c: CaseWhen =>
      val values = if (c.elseValue.isDefined) c.branches.map(_._2) ++ c.elseValue else Nil
      RecurseChildren(Seq(c.children.head), values, c.children.tail)
    case c: Coalesce => RecurseChildren(Seq(c.children.head), Nil, c.children.tail)
    case h: HigherOrderFunction => RecurseChildren(h.arguments)
    case other => RecurseChildren(other.children)
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      map: mutable.HashMap[ExpressionEquals, ExpressionStats] = equivalenceMap,
      conditional: Boolean = false,
      skipExpressions: mutable.Set[ExpressionEquals] = mutable.Set.empty[ExpressionEquals]
      ): Unit = {
    val skip = expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.find(_.isInstanceOf[LambdaVariable]).isDefined ||
      // `PlanExpression` wraps query plan. To compare query plans of `PlanExpression` on executor,
      // can cause error like NPE.
      (expr.find(_.isInstanceOf[PlanExpression[_]]).isDefined && TaskContext.get != null) ||
      // Specific set of expressions to skip, used for excluding common expressions from
      // conditional expressions
      skipExpressions.contains(ExpressionEquals(expr))

    if (!skip && !addExprToMap(expr, map, conditional)) {
      val recurseChildren = childrenToRecurse(expr)
      recurseChildren.alwaysChildren.foreach { child =>
        addExprTree(child, map, conditional, skipExpressions)
      }

      /**
       * If the `commonExpressions` already appears in the equivalence map, calling `addExprTree`
       * will increase the `useCount` and mark it as a common subexpression. Otherwise,
       * `addExprTree` will recursively add `commonExpressions` and its descendant to the
       * equivalence map, in case they also appear in other places. For example,
       * `If(a + b > 1, a + b + c, a + b + c)`, `a + b` also appears in the condition and should
       * be treated as common subexpression.
       */
      val commonExpressions = if (recurseChildren.commonChildren.nonEmpty) {
        findCommonExprs(recurseChildren.commonChildren)
      } else {
        Nil
      }
      commonExpressions.foreach(ce => addExprTree(ce.e, map, conditional, skipExpressions))

      if (SQLConf.get.subexpressionEliminationConditionalsEnabled) {
        val commonExpressionSet = mutable.Set[ExpressionEquals]()
        commonExpressions.foreach(ce => commonExpressionSet.add(ce))
        recurseChildren.conditionalChildren.foreach { cc =>
          addExprTree(cc, map, true, commonExpressionSet)
        }
      }
    }
  }

  /**
   * Returns the state of the given expression in the `equivalenceMap`. Returns None if there is no
   * equivalent expressions.
   */
  def getExprState(e: Expression): Option[ExpressionStats] = {
    equivalenceMap.get(ExpressionEquals(e))
  }

  // Exposed for testing.
  private[sql] def getAllExprStates(count: Int = 0): Seq[ExpressionStats] = {
    equivalenceMap.values.filter(_.getUseCount > count).toSeq.sortBy(_.height)
  }

  /**
   * Returns a sequence of expressions that more than one equivalent expressions.
   */
  def getCommonSubexpressions: Seq[Expression] = {
    getAllExprStates(1).map(_.expr)
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb = new java.lang.StringBuilder()
    sb.append("Equivalent expressions:\n")
    equivalenceMap.values.filter(stats => all || stats.getUseCount > 1).foreach { stats =>
      sb.append("  ")
        .append(s"${stats.expr}: useCount = ${stats.useCount} ")
        .append(s"conditionalUseCount = ${stats.conditionalUseCount}")
        .append('\n')
    }
    sb.toString()
  }
}

/**
 * Wrapper around an Expression that provides semantic equality.
 */
case class ExpressionEquals(e: Expression) {
  override def equals(o: Any): Boolean = o match {
    case other: ExpressionEquals => e.semanticEquals(other.e)
    case _ => false
  }

  override def hashCode: Int = e.semanticHash()
}

/**
 * A wrapper in place of using Seq[Expression] to record a group of equivalent expressions.
 *
 * This saves a lot of memory when there are a lot of expressions in a same equivalence group.
 * Instead of appending to a mutable list/buffer of Expressions, just update the "flattened"
 * useCount in this wrapper in-place.
 */
case class ExpressionStats(expr: Expression)(
    var useCount: Int = 1,
    var conditionalUseCount: Int = 0) {
  // This is used to do a fast pre-check for child-parent relationship. For example, expr1 can
  // only be a parent of expr2 if expr1.height is larger than expr2.height.
  lazy val height = getHeight(expr)

  private def getHeight(tree: Expression): Int = {
    tree.children.map(getHeight).reduceOption(_ max _).getOrElse(0) + 1
  }

  def getUseCount(): Int = if (useCount > 0) {
    useCount + conditionalUseCount
  } else {
    0
  }
}

/**
 * A wrapper for the different types of children of expressions. `alwaysChildren` are child
 * expressions that will always be evaluated and should be considered for subexpressions.
 * `commonChildren` are children such that if there are any common expressions among them, those
 * should be considered for subexpressions. `conditionalChildren` are children that are
 * conditionally evaluated, such as in If, CaseWhen, or Coalesce expressions, and should only
 * be considered for subexpressions if they are evaluated non-conditionally elsewhere.
 */
case class RecurseChildren(
    alwaysChildren: Seq[Expression],
    commonChildren: Seq[Expression] = Nil,
    conditionalChildren: Seq[Expression] = Nil
  )

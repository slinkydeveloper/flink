/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.optimizer.costs;

import org.apache.flink.optimizer.dag.EstimateProvider;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the cost formulas in the {@link DefaultCostEstimator}. Most of the tests establish
 * relative relationships.
 */
public class DefaultCostEstimatorTest {

    // estimates

    private static final long SMALL_DATA_SIZE = 10000;
    private static final long SMALL_RECORD_COUNT = 100;

    private static final long MEDIUM_DATA_SIZE = 500000000L;
    private static final long MEDIUM_RECORD_COUNT = 500000L;

    private static final long BIG_DATA_SIZE = 100000000000L;
    private static final long BIG_RECORD_COUNT = 100000000L;

    private static final EstimateProvider UNKNOWN_ESTIMATES = new UnknownEstimates();
    private static final EstimateProvider ZERO_ESTIMATES = new Estimates(0, 0);
    private static final EstimateProvider SMALL_ESTIMATES =
            new Estimates(SMALL_DATA_SIZE, SMALL_RECORD_COUNT);
    private static final EstimateProvider MEDIUM_ESTIMATES =
            new Estimates(MEDIUM_DATA_SIZE, MEDIUM_RECORD_COUNT);
    private static final EstimateProvider BIG_ESTIMATES =
            new Estimates(BIG_DATA_SIZE, BIG_RECORD_COUNT);

    private final CostEstimator costEstimator = new DefaultCostEstimator();

    // --------------------------------------------------------------------------------------------

    @Test
    public void testShipStrategiesIsolated() {
        testShipStrategiesIsolated(UNKNOWN_ESTIMATES, 1);
        testShipStrategiesIsolated(UNKNOWN_ESTIMATES, 10);
        testShipStrategiesIsolated(ZERO_ESTIMATES, 1);
        testShipStrategiesIsolated(ZERO_ESTIMATES, 10);
        testShipStrategiesIsolated(SMALL_ESTIMATES, 1);
        testShipStrategiesIsolated(SMALL_ESTIMATES, 10);
        testShipStrategiesIsolated(BIG_ESTIMATES, 1);
        testShipStrategiesIsolated(BIG_ESTIMATES, 10);
    }

    private void testShipStrategiesIsolated(EstimateProvider estimates, int targetParallelism) {
        Costs random = new Costs();
        costEstimator.addRandomPartitioningCost(estimates, random);

        Costs hash = new Costs();
        costEstimator.addHashPartitioningCost(estimates, hash);

        Costs range = new Costs();
        costEstimator.addRangePartitionCost(estimates, range);

        Costs broadcast = new Costs();
        costEstimator.addBroadcastCost(estimates, targetParallelism, broadcast);

        int randomVsHash = random.compareTo(hash);
        int hashVsRange = hash.compareTo(range);
        int hashVsBroadcast = hash.compareTo(broadcast);
        int rangeVsBroadcast = range.compareTo(broadcast);

        // repartition random is at most as expensive as hash partitioning
        assertThat(randomVsHash <= 0).isTrue();

        // range partitioning is always more expensive than hash partitioning
        assertThat(hashVsRange < 0).isTrue();

        // broadcasting is always more expensive than hash partitioning
        if (targetParallelism > 1) {
            assertThat(hashVsBroadcast < 0).isTrue();
        } else {
            assertThat(hashVsBroadcast <= 0).isTrue();
        }

        // range partitioning is not more expensive than broadcasting
        if (targetParallelism > 1) {
            assertThat(rangeVsBroadcast < 0).isTrue();
        }
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testShipStrategyCombinationsPlain() {
        Costs hashBothSmall = new Costs();
        Costs hashSmallAndLarge = new Costs();
        Costs hashBothLarge = new Costs();

        Costs hashSmallBcLarge10 = new Costs();
        Costs hashLargeBcSmall10 = new Costs();

        Costs hashSmallBcLarge1000 = new Costs();
        Costs hashLargeBcSmall1000 = new Costs();

        Costs forwardSmallBcLarge10 = new Costs();
        Costs forwardLargeBcSmall10 = new Costs();

        Costs forwardSmallBcLarge1000 = new Costs();
        Costs forwardLargeBcSmall1000 = new Costs();

        costEstimator.addHashPartitioningCost(MEDIUM_ESTIMATES, hashBothSmall);
        costEstimator.addHashPartitioningCost(MEDIUM_ESTIMATES, hashBothSmall);

        costEstimator.addHashPartitioningCost(MEDIUM_ESTIMATES, hashSmallAndLarge);
        costEstimator.addHashPartitioningCost(BIG_ESTIMATES, hashSmallAndLarge);

        costEstimator.addHashPartitioningCost(BIG_ESTIMATES, hashBothLarge);
        costEstimator.addHashPartitioningCost(BIG_ESTIMATES, hashBothLarge);

        costEstimator.addHashPartitioningCost(MEDIUM_ESTIMATES, hashSmallBcLarge10);
        costEstimator.addBroadcastCost(BIG_ESTIMATES, 10, hashSmallBcLarge10);

        costEstimator.addHashPartitioningCost(BIG_ESTIMATES, hashLargeBcSmall10);
        costEstimator.addBroadcastCost(MEDIUM_ESTIMATES, 10, hashLargeBcSmall10);

        costEstimator.addHashPartitioningCost(MEDIUM_ESTIMATES, hashSmallBcLarge1000);
        costEstimator.addBroadcastCost(BIG_ESTIMATES, 1000, hashSmallBcLarge1000);

        costEstimator.addHashPartitioningCost(BIG_ESTIMATES, hashLargeBcSmall1000);
        costEstimator.addBroadcastCost(MEDIUM_ESTIMATES, 1000, hashLargeBcSmall1000);

        costEstimator.addBroadcastCost(BIG_ESTIMATES, 10, forwardSmallBcLarge10);

        costEstimator.addBroadcastCost(MEDIUM_ESTIMATES, 10, forwardLargeBcSmall10);

        costEstimator.addBroadcastCost(BIG_ESTIMATES, 1000, forwardSmallBcLarge1000);

        costEstimator.addBroadcastCost(MEDIUM_ESTIMATES, 1000, forwardLargeBcSmall1000);

        // hash cost is roughly monotonous
        assertThat(hashBothSmall.compareTo(hashSmallAndLarge) < 0).isTrue();
        assertThat(hashSmallAndLarge.compareTo(hashBothLarge) < 0).isTrue();

        // broadcast the smaller is better
        assertThat(hashLargeBcSmall10.compareTo(hashSmallBcLarge10) < 0).isTrue();
        assertThat(forwardLargeBcSmall10.compareTo(forwardSmallBcLarge10) < 0).isTrue();
        assertThat(hashLargeBcSmall1000.compareTo(hashSmallBcLarge1000) < 0).isTrue();
        assertThat(forwardLargeBcSmall1000.compareTo(forwardSmallBcLarge1000) < 0).isTrue();

        // broadcasting small and forwarding large is better than partition both, given size
        // difference
        assertThat(forwardLargeBcSmall10.compareTo(hashSmallAndLarge) < 0).isTrue();

        // broadcasting too far is expensive again
        assertThat(forwardLargeBcSmall1000.compareTo(hashSmallAndLarge) > 0).isTrue();

        // assert weight is respected
        assertThat(hashSmallBcLarge10.compareTo(hashSmallBcLarge1000) < 0).isTrue();
        assertThat(hashLargeBcSmall10.compareTo(hashLargeBcSmall1000) < 0).isTrue();
        assertThat(forwardSmallBcLarge10.compareTo(forwardSmallBcLarge1000) < 0).isTrue();
        assertThat(forwardLargeBcSmall10.compareTo(forwardLargeBcSmall1000) < 0).isTrue();

        // forward versus hash
        assertThat(forwardSmallBcLarge10.compareTo(hashSmallBcLarge10) < 0).isTrue();
        assertThat(forwardSmallBcLarge1000.compareTo(hashSmallBcLarge1000) < 0).isTrue();
        assertThat(forwardLargeBcSmall10.compareTo(hashLargeBcSmall10) < 0).isTrue();
        assertThat(forwardLargeBcSmall1000.compareTo(hashLargeBcSmall1000) < 0).isTrue();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testShipStrategyCombinationsWithUnknowns() {
        testShipStrategyCombinationsWithUnknowns(UNKNOWN_ESTIMATES);
        testShipStrategyCombinationsWithUnknowns(ZERO_ESTIMATES);
        testShipStrategyCombinationsWithUnknowns(SMALL_ESTIMATES);
        testShipStrategyCombinationsWithUnknowns(MEDIUM_ESTIMATES);
        testShipStrategyCombinationsWithUnknowns(BIG_ESTIMATES);
    }

    private void testShipStrategyCombinationsWithUnknowns(EstimateProvider knownEstimates) {
        Costs hashBoth = new Costs();
        Costs bcKnown10 = new Costs();
        Costs bcUnknown10 = new Costs();
        Costs bcKnown1000 = new Costs();
        Costs bcUnknown1000 = new Costs();

        costEstimator.addHashPartitioningCost(knownEstimates, hashBoth);
        costEstimator.addHashPartitioningCost(UNKNOWN_ESTIMATES, hashBoth);

        costEstimator.addBroadcastCost(knownEstimates, 10, bcKnown10);

        costEstimator.addBroadcastCost(UNKNOWN_ESTIMATES, 10, bcUnknown10);

        costEstimator.addBroadcastCost(knownEstimates, 1000, bcKnown1000);

        costEstimator.addBroadcastCost(UNKNOWN_ESTIMATES, 1000, bcUnknown1000);

        // if we do not know one of them, hashing both should be cheaper than anything
        assertThat(hashBoth.compareTo(bcKnown10) < 0).isTrue();
        assertThat(hashBoth.compareTo(bcUnknown10) < 0).isTrue();
        assertThat(hashBoth.compareTo(bcKnown1000) < 0).isTrue();
        assertThat(hashBoth.compareTo(bcUnknown1000) < 0).isTrue();

        // there should be no bias in broadcasting a known or unknown size input
        assertThat(bcKnown10.compareTo(bcUnknown10) == 0).isTrue();
        assertThat(bcKnown1000.compareTo(bcUnknown1000) == 0).isTrue();

        // replication factor does matter
        assertThat(bcKnown10.compareTo(bcKnown1000) < 0).isTrue();
        assertThat(bcUnknown10.compareTo(bcUnknown1000) < 0).isTrue();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testJoinCostFormulasPlain() {

        // hash join costs

        Costs hashBothSmall = new Costs();
        Costs hashBothLarge = new Costs();
        Costs hashSmallBuild = new Costs();
        Costs hashLargeBuild = new Costs();

        costEstimator.addHybridHashCosts(SMALL_ESTIMATES, BIG_ESTIMATES, hashSmallBuild, 1);
        costEstimator.addHybridHashCosts(BIG_ESTIMATES, SMALL_ESTIMATES, hashLargeBuild, 1);
        costEstimator.addHybridHashCosts(SMALL_ESTIMATES, SMALL_ESTIMATES, hashBothSmall, 1);
        costEstimator.addHybridHashCosts(BIG_ESTIMATES, BIG_ESTIMATES, hashBothLarge, 1);

        assertThat(hashBothSmall.compareTo(hashSmallBuild) < 0).isTrue();
        assertThat(hashSmallBuild.compareTo(hashLargeBuild) < 0).isTrue();
        assertThat(hashLargeBuild.compareTo(hashBothLarge) < 0).isTrue();

        // merge join costs

        Costs mergeBothSmall = new Costs();
        Costs mergeBothLarge = new Costs();
        Costs mergeSmallFirst = new Costs();
        Costs mergeSmallSecond = new Costs();

        costEstimator.addLocalSortCost(SMALL_ESTIMATES, mergeSmallFirst);
        costEstimator.addLocalSortCost(BIG_ESTIMATES, mergeSmallFirst);
        costEstimator.addLocalMergeCost(SMALL_ESTIMATES, BIG_ESTIMATES, mergeSmallFirst, 1);

        costEstimator.addLocalSortCost(BIG_ESTIMATES, mergeSmallSecond);
        costEstimator.addLocalSortCost(SMALL_ESTIMATES, mergeSmallSecond);
        costEstimator.addLocalMergeCost(BIG_ESTIMATES, SMALL_ESTIMATES, mergeSmallSecond, 1);

        costEstimator.addLocalSortCost(SMALL_ESTIMATES, mergeBothSmall);
        costEstimator.addLocalSortCost(SMALL_ESTIMATES, mergeBothSmall);
        costEstimator.addLocalMergeCost(SMALL_ESTIMATES, SMALL_ESTIMATES, mergeBothSmall, 1);

        costEstimator.addLocalSortCost(BIG_ESTIMATES, mergeBothLarge);
        costEstimator.addLocalSortCost(BIG_ESTIMATES, mergeBothLarge);
        costEstimator.addLocalMergeCost(BIG_ESTIMATES, BIG_ESTIMATES, mergeBothLarge, 1);

        assertThat(mergeBothSmall.compareTo(mergeSmallFirst) < 0).isTrue();
        assertThat(mergeBothSmall.compareTo(mergeSmallSecond) < 0).isTrue();
        assertThat(mergeSmallFirst.compareTo(mergeSmallSecond) == 0).isTrue();
        assertThat(mergeSmallFirst.compareTo(mergeBothLarge) < 0).isTrue();
        assertThat(mergeSmallSecond.compareTo(mergeBothLarge) < 0).isTrue();

        // compare merge join and hash join costs

        assertThat(hashBothSmall.compareTo(mergeBothSmall) < 0).isTrue();
        assertThat(hashBothLarge.compareTo(mergeBothLarge) < 0).isTrue();
        assertThat(hashSmallBuild.compareTo(mergeSmallFirst) < 0).isTrue();
        assertThat(hashSmallBuild.compareTo(mergeSmallSecond) < 0).isTrue();
        assertThat(hashLargeBuild.compareTo(mergeSmallFirst) < 0).isTrue();
        assertThat(hashLargeBuild.compareTo(mergeSmallSecond) < 0).isTrue();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testJoinCostFormulasWithWeights() {
        testJoinCostFormulasWithWeights(UNKNOWN_ESTIMATES, SMALL_ESTIMATES);
        testJoinCostFormulasWithWeights(SMALL_ESTIMATES, UNKNOWN_ESTIMATES);
        testJoinCostFormulasWithWeights(UNKNOWN_ESTIMATES, MEDIUM_ESTIMATES);
        testJoinCostFormulasWithWeights(MEDIUM_ESTIMATES, UNKNOWN_ESTIMATES);
        testJoinCostFormulasWithWeights(BIG_ESTIMATES, MEDIUM_ESTIMATES);
        testJoinCostFormulasWithWeights(MEDIUM_ESTIMATES, BIG_ESTIMATES);
    }

    private void testJoinCostFormulasWithWeights(EstimateProvider e1, EstimateProvider e2) {
        Costs hf1 = new Costs();
        Costs hf5 = new Costs();
        Costs hs1 = new Costs();
        Costs hs5 = new Costs();
        Costs mm1 = new Costs();
        Costs mm5 = new Costs();

        costEstimator.addHybridHashCosts(e1, e2, hf1, 1);
        costEstimator.addHybridHashCosts(e1, e2, hf5, 5);
        costEstimator.addHybridHashCosts(e2, e1, hs1, 1);
        costEstimator.addHybridHashCosts(e2, e1, hs5, 5);

        costEstimator.addLocalSortCost(e1, mm1);
        costEstimator.addLocalSortCost(e2, mm1);
        costEstimator.addLocalMergeCost(e1, e2, mm1, 1);

        costEstimator.addLocalSortCost(e1, mm5);
        costEstimator.addLocalSortCost(e2, mm5);
        mm5.multiplyWith(5);
        costEstimator.addLocalMergeCost(e1, e2, mm5, 5);

        // weight 1 versus weight 5
        assertThat(hf1.compareTo(hf5) < 0).isTrue();
        assertThat(hs1.compareTo(hs5) < 0).isTrue();
        assertThat(mm1.compareTo(mm5) < 0).isTrue();

        // hash versus merge
        assertThat(hf1.compareTo(mm1) < 0).isTrue();
        assertThat(hs1.compareTo(mm1) < 0).isTrue();
        assertThat(hf5.compareTo(mm5) < 0).isTrue();
        assertThat(hs5.compareTo(mm5) < 0).isTrue();
    }

    // --------------------------------------------------------------------------------------------

    @Test
    public void testHashJoinCostFormulasWithCaches() {

        Costs hashBothUnknown10 = new Costs();
        Costs hashBothUnknownCached10 = new Costs();

        Costs hashBothSmall10 = new Costs();
        Costs hashBothSmallCached10 = new Costs();

        Costs hashSmallLarge10 = new Costs();
        Costs hashSmallLargeCached10 = new Costs();

        Costs hashLargeSmall10 = new Costs();
        Costs hashLargeSmallCached10 = new Costs();

        Costs hashLargeSmall1 = new Costs();
        Costs hashLargeSmallCached1 = new Costs();

        costEstimator.addHybridHashCosts(
                UNKNOWN_ESTIMATES, UNKNOWN_ESTIMATES, hashBothUnknown10, 10);
        costEstimator.addCachedHybridHashCosts(
                UNKNOWN_ESTIMATES, UNKNOWN_ESTIMATES, hashBothUnknownCached10, 10);

        costEstimator.addHybridHashCosts(MEDIUM_ESTIMATES, MEDIUM_ESTIMATES, hashBothSmall10, 10);
        costEstimator.addCachedHybridHashCosts(
                MEDIUM_ESTIMATES, MEDIUM_ESTIMATES, hashBothSmallCached10, 10);

        costEstimator.addHybridHashCosts(MEDIUM_ESTIMATES, BIG_ESTIMATES, hashSmallLarge10, 10);
        costEstimator.addCachedHybridHashCosts(
                MEDIUM_ESTIMATES, BIG_ESTIMATES, hashSmallLargeCached10, 10);

        costEstimator.addHybridHashCosts(BIG_ESTIMATES, MEDIUM_ESTIMATES, hashLargeSmall10, 10);
        costEstimator.addCachedHybridHashCosts(
                BIG_ESTIMATES, MEDIUM_ESTIMATES, hashLargeSmallCached10, 10);

        costEstimator.addHybridHashCosts(BIG_ESTIMATES, MEDIUM_ESTIMATES, hashLargeSmall1, 1);
        costEstimator.addCachedHybridHashCosts(
                BIG_ESTIMATES, MEDIUM_ESTIMATES, hashLargeSmallCached1, 1);

        // cached variant is always cheaper
        assertThat(hashBothUnknown10.compareTo(hashBothUnknownCached10) > 0).isTrue();
        assertThat(hashBothSmall10.compareTo(hashBothSmallCached10) > 0).isTrue();
        assertThat(hashSmallLarge10.compareTo(hashSmallLargeCached10) > 0).isTrue();
        assertThat(hashLargeSmall10.compareTo(hashLargeSmallCached10) > 0).isTrue();

        // caching the large side is better, because then the small one is the one with additional
        // I/O
        assertThat(hashLargeSmallCached10.compareTo(hashSmallLargeCached10) < 0).isTrue();

        // a weight of one makes the caching the same as the non-cached variant
        assertThat(hashLargeSmall1.compareTo(hashLargeSmallCached1) == 0).isTrue();
    }

    // --------------------------------------------------------------------------------------------
    //  Estimate providers
    // --------------------------------------------------------------------------------------------

    private static final class UnknownEstimates implements EstimateProvider {

        @Override
        public long getEstimatedOutputSize() {
            return -1;
        }

        @Override
        public long getEstimatedNumRecords() {
            return -1;
        }

        @Override
        public float getEstimatedAvgWidthPerOutputRecord() {
            return -1.0f;
        }
    }

    private static final class Estimates implements EstimateProvider {

        private final long size;
        private final long records;
        private final float width;

        public Estimates(long size, long records) {
            this(size, records, -1.0f);
        }

        public Estimates(long size, long records, float width) {
            this.size = size;
            this.records = records;
            this.width = width;
        }

        @Override
        public long getEstimatedOutputSize() {
            return this.size;
        }

        @Override
        public long getEstimatedNumRecords() {
            return this.records;
        }

        @Override
        public float getEstimatedAvgWidthPerOutputRecord() {
            return this.width;
        }
    }
}

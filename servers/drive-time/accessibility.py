"""Enhanced Two-Step Floating Catchment Area (E2SFCA) accessibility scoring.

The E2SFCA method measures spatial accessibility by considering both supply
(e.g., hospital beds) and demand (e.g., population) within a travel-time
catchment. It produces a score for each demand point that reflects how much
healthcare capacity is accessible relative to competing demand.

Reference:
    Luo, W. & Qi, Y. (2009). An enhanced two-step floating catchment area
    (E2SFCA) method for measuring spatial accessibility to primary care
    physicians. Health & Place, 15(4), 1100-1107.
"""

import statistics


def compute_e2sfca(
    travel_time_matrix: list[list[float | None]],
    demand_populations: list[float],
    supply_capacities: list[float],
    catchment_minutes: float,
) -> list[float]:
    """Compute Enhanced 2SFCA accessibility scores.

    Args:
        travel_time_matrix: Matrix of travel times in minutes.
            Rows = demand points, columns = supply points.
            None means unreachable.
        demand_populations: Population/weight for each demand point.
        supply_capacities: Capacity (e.g., beds) for each supply point.
        catchment_minutes: Maximum travel time for the catchment area.

    Returns:
        List of accessibility scores, one per demand point.
        Higher score = better access.
    """
    num_demand = len(demand_populations)
    num_supply = len(supply_capacities)

    # Step 1: For each supply point j, compute R_j = S_j / sum(D_k) for all
    # demand points k within the catchment.
    supply_ratios: list[float] = []
    for j in range(num_supply):
        total_demand = 0.0
        for i in range(num_demand):
            travel_time = travel_time_matrix[i][j]
            if travel_time is not None and travel_time <= catchment_minutes:
                total_demand += demand_populations[i]

        if total_demand > 0:
            supply_ratios.append(supply_capacities[j] / total_demand)
        else:
            supply_ratios.append(0.0)

    # Step 2: For each demand point i, sum R_j for all supply points j within
    # the catchment.
    scores: list[float] = []
    for i in range(num_demand):
        score = 0.0
        for j in range(num_supply):
            travel_time = travel_time_matrix[i][j]
            if travel_time is not None and travel_time <= catchment_minutes:
                score += supply_ratios[j]
        scores.append(score)

    return scores


def summarize_scores(scores: list[float]) -> dict:
    """Compute summary statistics for accessibility scores."""
    if not scores:
        return {
            "mean": 0.0,
            "median": 0.0,
            "min": 0.0,
            "max": 0.0,
            "std": 0.0,
            "points_with_zero_access": 0,
        }

    return {
        "mean": statistics.mean(scores),
        "median": statistics.median(scores),
        "min": min(scores),
        "max": max(scores),
        "std": statistics.stdev(scores) if len(scores) > 1 else 0.0,
        "points_with_zero_access": sum(1 for s in scores if s == 0.0),
    }

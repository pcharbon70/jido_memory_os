defmodule Jido.MemoryOS.Retrieval.Planner do
  @moduledoc """
  Deterministic retrieval planner that converts query intent into per-tier fanout.
  """

  alias Jido.MemoryOS.Query

  @type tier :: :short | :mid | :long

  @type plan :: %{
          mode: Query.tier_mode(),
          primary_tiers: [tier()],
          fallback_tiers: [tier()],
          fanout: %{optional(tier()) => pos_integer()},
          minimum_results: pos_integer(),
          sparse_fallback?: boolean()
        }

  @doc """
  Computes base retrieval plan and tier fanout allocations.
  """
  @spec plan(Query.t(), map()) :: {:ok, plan()}
  def plan(%Query{} = query, _retrieval_config) do
    primary_tiers = primary_tiers(query.tier_mode)
    fallback_tiers = fallback_tiers(query.tier_mode)

    fanout =
      allocate_fanout(
        primary_tiers,
        query.candidate_fanout,
        weights(query.tier_mode)
      )

    {:ok,
     %{
       mode: query.tier_mode,
       primary_tiers: primary_tiers,
       fallback_tiers: fallback_tiers,
       fanout: fanout,
       minimum_results: query.limit,
       sparse_fallback?: fallback_tiers != []
     }}
  end

  @doc """
  Expands plan tiers when sparse candidate sets are observed.
  """
  @spec apply_sparse_fallback(plan(), non_neg_integer(), pos_integer()) :: plan()
  def apply_sparse_fallback(plan, candidate_count, limit) do
    if plan.sparse_fallback? and candidate_count < limit and plan.fallback_tiers != [] do
      needed = max(1, limit - candidate_count)

      fallback_fanout =
        allocate_fanout(plan.fallback_tiers, needed * 2, fallback_weights(plan.fallback_tiers))

      %{
        plan
        | primary_tiers: plan.primary_tiers ++ plan.fallback_tiers,
          fallback_tiers: [],
          fanout:
            Map.merge(plan.fanout, fallback_fanout, fn _tier, existing, extra ->
              max(existing, extra)
            end),
          sparse_fallback?: false
      }
    else
      plan
    end
  end

  @spec primary_tiers(Query.tier_mode()) :: [tier()]
  defp primary_tiers(:short), do: [:short]
  defp primary_tiers(:mid), do: [:mid]
  defp primary_tiers(:long), do: [:long]
  defp primary_tiers(:long_priority), do: [:long, :mid]
  defp primary_tiers(:tiered), do: [:short, :mid, :long]
  defp primary_tiers(:hybrid), do: [:short, :mid, :long]
  defp primary_tiers(:all), do: [:short, :mid, :long]

  @spec fallback_tiers(Query.tier_mode()) :: [tier()]
  defp fallback_tiers(:short), do: [:mid, :long]
  defp fallback_tiers(:mid), do: [:short, :long]
  defp fallback_tiers(:long), do: [:mid, :short]
  defp fallback_tiers(:long_priority), do: [:short]
  defp fallback_tiers(:tiered), do: []
  defp fallback_tiers(:hybrid), do: []
  defp fallback_tiers(:all), do: []

  @spec weights(Query.tier_mode()) :: %{optional(tier()) => number()}
  defp weights(:short), do: %{short: 1.0}
  defp weights(:mid), do: %{mid: 1.0}
  defp weights(:long), do: %{long: 1.0}
  defp weights(:long_priority), do: %{long: 0.7, mid: 0.3}
  defp weights(:tiered), do: %{short: 0.45, mid: 0.35, long: 0.2}
  defp weights(:hybrid), do: %{short: 0.5, mid: 0.3, long: 0.2}
  defp weights(:all), do: %{short: 0.34, mid: 0.33, long: 0.33}

  @spec fallback_weights([tier()]) :: %{optional(tier()) => number()}
  defp fallback_weights([]), do: %{}

  defp fallback_weights(tiers) do
    weight = 1.0 / length(tiers)
    Map.new(tiers, &{&1, weight})
  end

  @spec allocate_fanout([tier()], pos_integer(), map()) :: %{optional(tier()) => pos_integer()}
  defp allocate_fanout([], _total, _weights), do: %{}

  defp allocate_fanout(tiers, total, weights) do
    base_total = max(total, length(tiers))

    initial =
      tiers
      |> Enum.reduce(%{}, fn tier, acc ->
        weight = Map.get(weights, tier, 1.0 / length(tiers))
        Map.put(acc, tier, max(1, trunc(base_total * weight)))
      end)

    current_total = Enum.reduce(initial, 0, fn {_tier, value}, acc -> acc + value end)

    if current_total >= base_total do
      initial
    else
      distribute_remaining(initial, tiers, base_total - current_total)
    end
  end

  @spec distribute_remaining(map(), [tier()], non_neg_integer()) :: map()
  defp distribute_remaining(fanout, _tiers, 0), do: fanout

  defp distribute_remaining(fanout, tiers, remaining) do
    tiers
    |> Stream.cycle()
    |> Enum.take(remaining)
    |> Enum.reduce(fanout, fn tier, acc -> Map.update!(acc, tier, &(&1 + 1)) end)
  end
end

defmodule Jido.MemoryOS.Retrieval.Ranker do
  @moduledoc """
  Hybrid deterministic ranker with pluggable semantic providers.
  """

  alias Jido.MemoryOS.Query
  alias Jido.MemoryOS.Retrieval.Candidate
  alias Jido.MemoryOS.Retrieval.SemanticProvider.Lexical

  @type ranked_candidate :: map()

  @doc """
  Scores and ranks normalized candidates.
  """
  @spec rank(Query.t(), [Candidate.t()], map(), keyword()) :: {:ok, map()} | {:error, term()}
  def rank(%Query{} = query, candidates, retrieval_config, opts \\ []) do
    lexical_scores = lexical_scores(query, candidates)

    with {:ok, semantic_scores, semantic_meta} <-
           semantic_scores(query, candidates, lexical_scores, opts) do
      ranked =
        candidates
        |> Enum.map(fn candidate ->
          features =
            feature_scores(
              query,
              candidate,
              lexical_scores,
              semantic_scores,
              retrieval_config
            )

          %{
            candidate: candidate,
            record: candidate.record,
            features: features,
            tier: candidate.tier
          }
        end)
        |> sort_ranked()

      {:ok,
       %{
         ranked: ranked,
         semantic: semantic_meta
       }}
    end
  end

  @spec lexical_scores(Query.t(), [Candidate.t()]) :: %{optional(String.t()) => number()}
  defp lexical_scores(query, candidates) do
    {:ok, scores} = Lexical.score(query, candidates, [])
    scores
  end

  @spec semantic_scores(Query.t(), [Candidate.t()], map(), keyword()) ::
          {:ok, map(), map()} | {:error, term()}
  defp semantic_scores(query, candidates, lexical_scores, opts) do
    provider = provider_module(query, opts)
    timeout_ms = query.semantic_timeout_ms

    task = Task.async(fn -> provider.score(query, candidates, opts) end)

    case Task.yield(task, timeout_ms) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, scores}} ->
        {:ok, scores,
         %{
           provider: provider,
           degraded?: false,
           reason: nil
         }}

      {:ok, {:error, reason}} ->
        {:ok, lexical_scores,
         %{
           provider: Lexical,
           degraded?: true,
           reason: {:provider_error, provider, reason}
         }}

      nil ->
        {:ok, lexical_scores,
         %{
           provider: Lexical,
           degraded?: true,
           reason: {:provider_timeout, provider, timeout_ms}
         }}
    end
  end

  @spec provider_module(Query.t(), keyword()) :: module()
  defp provider_module(query, opts) do
    provider = query.semantic_provider || Keyword.get(opts, :semantic_provider, Lexical)

    if function_exported?(provider, :score, 3) do
      provider
    else
      Lexical
    end
  end

  @spec feature_scores(Query.t(), Candidate.t(), map(), map(), map()) :: map()
  defp feature_scores(query, candidate, lexical_scores, semantic_scores, retrieval_config) do
    lexical = Map.get(lexical_scores, candidate.key, 0.0)
    semantic = Map.get(semantic_scores, candidate.key, lexical)
    recency = clamp(candidate.recency, 0.0, 1.0)
    heat = clamp(candidate.heat, 0.0, 1.0)
    persona = persona_score(candidate, query.persona_keys)
    topic = topic_score(candidate, query.topic_keys)
    tier_bias = tier_bias(candidate.tier)

    lexical_weight = get_in(retrieval_config, [:ranking, :lexical_weight]) || 0.7
    semantic_weight = get_in(retrieval_config, [:ranking, :semantic_weight]) || 0.3

    final_score =
      Float.round(
        lexical_weight * lexical + semantic_weight * semantic + 0.12 * recency + 0.08 * heat +
          0.06 * persona + 0.04 * topic + tier_bias,
        4
      )

    %{
      lexical: lexical,
      semantic: semantic,
      recency: recency,
      heat: heat,
      persona: persona,
      topic: topic,
      tier_bias: tier_bias,
      final_score: final_score
    }
  end

  @spec sort_ranked([ranked_candidate()]) :: [ranked_candidate()]
  defp sort_ranked(ranked) do
    Enum.sort(ranked, fn left, right ->
      cond do
        left.features.final_score > right.features.final_score ->
          true

        left.features.final_score < right.features.final_score ->
          false

        (left.record.observed_at || 0) > (right.record.observed_at || 0) ->
          true

        (left.record.observed_at || 0) < (right.record.observed_at || 0) ->
          false

        true ->
          left.record.id <= right.record.id
      end
    end)
  end

  @spec persona_score(Candidate.t(), [String.t()]) :: number()
  defp persona_score(_candidate, []), do: 0.5

  defp persona_score(candidate, persona_keys) do
    if Enum.any?(candidate.persona_keys, &(&1 in persona_keys)), do: 1.0, else: 0.0
  end

  @spec topic_score(Candidate.t(), [String.t()]) :: number()
  defp topic_score(_candidate, []), do: 0.5

  defp topic_score(candidate, topic_keys) do
    if Enum.any?(candidate.topic_keys, &(&1 in topic_keys)), do: 1.0, else: 0.0
  end

  @spec tier_bias(:short | :mid | :long) :: number()
  defp tier_bias(:short), do: 0.1
  defp tier_bias(:mid), do: 0.05
  defp tier_bias(:long), do: 0.0

  @spec clamp(number(), number(), number()) :: number()
  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value
end

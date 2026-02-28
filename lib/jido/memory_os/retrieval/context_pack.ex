defmodule Jido.MemoryOS.Retrieval.ContextPack do
  @moduledoc """
  Builds token-bounded, provenance-aware context packs from ranked candidates.
  """

  alias Jido.MemoryOS.Query

  @doc """
  Builds a grouped context pack and persona synthesis hints.
  """
  @spec build(Query.t(), [map()]) :: map()
  def build(%Query{} = query, ranked) do
    token_budget = query.context_token_budget

    {entries, tokens_used, truncated?} = take_with_token_budget(ranked, token_budget)

    grouped =
      entries
      |> Enum.group_by(fn entry -> {entry.tier, entry.topic} end)
      |> Enum.map(fn {{tier, topic}, topic_entries} ->
        %{
          tier: tier,
          topic: topic,
          entries: Enum.map(topic_entries, &Map.drop(&1, [:topic]))
        }
      end)
      |> Enum.sort_by(fn group -> {tier_order(group.tier), group.topic} end)

    %{
      token_budget: token_budget,
      tokens_used: tokens_used,
      truncated: truncated?,
      groups: grouped,
      persona_hints: synthesize_persona_hints(entries)
    }
  end

  @spec take_with_token_budget([map()], pos_integer()) :: {[map()], non_neg_integer(), boolean()}
  defp take_with_token_budget(ranked, token_budget) do
    ranked
    |> Enum.reduce({[], 0, false}, fn ranked_candidate, {acc, used, truncated?} ->
      candidate = ranked_candidate.candidate
      remaining = token_budget - used

      cond do
        remaining <= 0 ->
          {acc, used, true}

        candidate.estimated_tokens <= remaining ->
          entry =
            entry_from_candidate(ranked_candidate, candidate.text, candidate.estimated_tokens)

          {[entry | acc], used + candidate.estimated_tokens, truncated?}

        remaining >= 24 ->
          text = truncate_to_tokens(candidate.text, remaining)
          entry = entry_from_candidate(ranked_candidate, text, remaining)
          {[entry | acc], used + remaining, true}

        true ->
          {acc, used, true}
      end
    end)
    |> then(fn {entries, used, truncated?} -> {Enum.reverse(entries), used, truncated?} end)
  end

  @spec entry_from_candidate(map(), String.t(), pos_integer()) :: map()
  defp entry_from_candidate(ranked_candidate, text, tokens) do
    candidate = ranked_candidate.candidate

    %{
      id: candidate.id,
      namespace: candidate.namespace,
      tier: candidate.tier,
      topic: primary_topic(candidate.topic_keys),
      text: text,
      tokens: tokens,
      score: ranked_candidate.features.final_score,
      fact_key: candidate.fact_key,
      observed_at: candidate.observed_at,
      provenance: Map.merge(candidate.provenance, %{source_text: text})
    }
  end

  @spec synthesize_persona_hints([map()]) :: [map()]
  defp synthesize_persona_hints(entries) do
    entries
    |> Enum.filter(&(&1.tier == :long))
    |> Enum.reduce(%{}, fn entry, acc ->
      existing = Map.get(acc, entry.fact_key)

      if is_nil(existing) or entry.observed_at >= existing.observed_at do
        Map.put(acc, entry.fact_key, entry)
      else
        acc
      end
    end)
    |> Map.values()
    |> Enum.map(fn entry ->
      %{
        fact_key: entry.fact_key,
        hint: entry.text,
        confidence: confidence(entry.score),
        source: entry.provenance
      }
    end)
    |> Enum.sort_by(& &1.confidence, :desc)
  end

  @spec confidence(number()) :: number()
  defp confidence(score), do: clamp(Float.round(0.5 + score / 2, 4), 0.0, 1.0)

  @spec primary_topic([String.t()]) :: String.t()
  defp primary_topic([first | _]), do: first
  defp primary_topic(_), do: "general"

  @spec truncate_to_tokens(String.t(), pos_integer()) :: String.t()
  defp truncate_to_tokens(text, max_tokens) do
    max_chars = max(1, max_tokens * 4)

    if String.length(text) <= max_chars do
      text
    else
      String.slice(text, 0, max_chars - 1) <> "…"
    end
  end

  @spec tier_order(:short | :mid | :long) :: integer()
  defp tier_order(:short), do: 0
  defp tier_order(:mid), do: 1
  defp tier_order(:long), do: 2

  @spec clamp(number(), number(), number()) :: number()
  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value
end

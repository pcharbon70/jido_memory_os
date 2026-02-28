defmodule Jido.MemoryOS.Retrieval.SemanticProvider.Lexical do
  @moduledoc """
  Zero-dependency lexical semantic provider fallback.

  It approximates semantic similarity through token overlap and phrase matching.
  """

  @behaviour Jido.MemoryOS.Retrieval.SemanticProvider

  alias Jido.MemoryOS.Query
  alias Jido.MemoryOS.Retrieval.Candidate

  @impl true
  @spec score(Query.t(), [map()], keyword()) :: {:ok, %{optional(String.t()) => number()}}
  def score(%Query{} = query, candidates, _opts) do
    query_tokens = tokenize(query.text_contains)

    scores =
      candidates
      |> Enum.reduce(%{}, fn candidate, acc ->
        key = candidate.key || Candidate.candidate_key(candidate.namespace, candidate.id)
        score = lexical_similarity(candidate, query.text_contains, query_tokens)
        Map.put(acc, key, score)
      end)

    {:ok, scores}
  end

  @spec lexical_similarity(map(), String.t() | nil, MapSet.t(String.t())) :: number()
  defp lexical_similarity(_candidate, nil, _query_tokens), do: 0.5

  defp lexical_similarity(candidate, query_text, query_tokens) do
    candidate_tokens = tokenize(candidate.normalized_text)

    overlap_score =
      if MapSet.size(query_tokens) == 0 do
        0.0
      else
        intersection = MapSet.intersection(query_tokens, candidate_tokens)
        MapSet.size(intersection) / MapSet.size(query_tokens)
      end

    phrase_match =
      if is_binary(query_text) and query_text != "" and
           String.contains?(candidate.normalized_text, String.downcase(query_text)) do
        1.0
      else
        0.0
      end

    clamp(Float.round(0.65 * overlap_score + 0.35 * phrase_match, 4), 0.0, 1.0)
  end

  @spec tokenize(String.t() | nil) :: MapSet.t(String.t())
  defp tokenize(nil), do: MapSet.new()

  defp tokenize(text) do
    text
    |> String.downcase()
    |> String.split(~r/[^\p{L}\p{N}_]+/u, trim: true)
    |> Enum.reject(&(&1 == ""))
    |> MapSet.new()
  end

  @spec clamp(number(), number(), number()) :: number()
  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value
end

defmodule Jido.MemoryOS.Retrieval.Candidate do
  @moduledoc """
  Candidate normalization helpers for unified retrieval scoring.
  """

  alias Jido.MemoryOS.{Lifecycle, Metadata}

  @type t :: %{
          key: String.t(),
          tier: :short | :mid | :long,
          record: Jido.Memory.Record.t(),
          id: String.t(),
          namespace: String.t(),
          text: String.t(),
          normalized_text: String.t(),
          content_text: String.t(),
          observed_at: integer(),
          recency: number(),
          heat: number(),
          promotion_score: number(),
          persona_keys: [String.t()],
          topic_keys: [String.t()],
          fact_key: String.t(),
          chain_id: String.t() | nil,
          estimated_tokens: pos_integer(),
          provenance: map()
        }

  @doc """
  Normalizes one record into a cross-tier candidate shape.
  """
  @spec from_record(Jido.Memory.Record.t(), :short | :mid | :long, integer()) :: t()
  def from_record(record, tier, now) do
    text = normalized_text(record)
    content_text = normalize_content_text(record.content)

    {heat, promotion_score, chain_id, persona_keys} =
      case Metadata.from_record(record) do
        {:ok, mem_os} ->
          {
            clamp(mem_os.heat, 0.0, 1.0),
            clamp(mem_os.promotion_score, 0.0, 1.0),
            mem_os.chain_id,
            normalize_tags(mem_os.persona_keys)
          }

        {:error, _} ->
          {0.0, 0.0, nil, []}
      end

    topic_keys = extract_topic_keys(record.tags)
    fact_key = Lifecycle.fact_key(record)
    observed_at = record.observed_at || now
    recency = normalize_recency(observed_at, now)

    %{
      key: candidate_key(record.namespace, record.id),
      tier: tier,
      record: record,
      id: record.id,
      namespace: record.namespace,
      text: text,
      normalized_text: String.downcase(text),
      content_text: content_text,
      observed_at: observed_at,
      recency: recency,
      heat: heat,
      promotion_score: promotion_score,
      persona_keys: persona_keys,
      topic_keys: topic_keys,
      fact_key: fact_key,
      chain_id: chain_id,
      estimated_tokens: estimate_tokens(text <> " " <> content_text),
      provenance: %{
        source_id: record.id,
        source_namespace: record.namespace,
        tier: tier,
        fact_key: fact_key,
        chain_id: chain_id,
        observed_at: observed_at
      }
    }
  end

  @doc """
  Returns stable key used by providers/ranker.
  """
  @spec candidate_key(String.t(), String.t()) :: String.t()
  def candidate_key(namespace, id), do: namespace <> "::" <> id

  @spec normalized_text(Jido.Memory.Record.t()) :: String.t()
  defp normalized_text(record) do
    cond do
      is_binary(record.text) and String.trim(record.text) != "" -> String.trim(record.text)
      true -> normalize_content_text(record.content)
    end
  end

  @spec normalize_content_text(term()) :: String.t()
  defp normalize_content_text(content) when is_binary(content), do: String.trim(content)
  defp normalize_content_text(content), do: inspect(content)

  @spec extract_topic_keys(term()) :: [String.t()]
  defp extract_topic_keys(tags) do
    tags
    |> normalize_tags()
    |> Enum.filter(&String.starts_with?(&1, "topic:"))
    |> Enum.map(&String.replace_prefix(&1, "topic:", ""))
    |> Enum.reject(&(&1 == ""))
  end

  @spec normalize_tags(term()) :: [String.t()]
  defp normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_tags(_), do: []

  @spec normalize_recency(integer(), integer()) :: number()
  defp normalize_recency(observed_at, now) do
    age_ms = max(0, now - observed_at)
    clamp(1.0 - age_ms / 86_400_000, 0.0, 1.0)
  end

  @spec estimate_tokens(String.t()) :: pos_integer()
  defp estimate_tokens(text), do: max(1, div(String.length(text), 4))

  @spec clamp(number(), number(), number()) :: number()
  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value
end

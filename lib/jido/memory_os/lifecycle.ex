defmodule Jido.MemoryOS.Lifecycle do
  @moduledoc """
  Pure lifecycle helpers for tiered memory transitions.
  """

  alias Jido.Memory.Record
  alias Jido.MemoryOS.Metadata

  @doc """
  Normalizes short-tier ingest attrs and returns deterministic lifecycle metadata.
  """
  @spec normalize_short_event(map() | keyword(), String.t(), non_neg_integer(), integer()) :: %{
          attrs: map(),
          mem_os: map(),
          ingest_meta: map(),
          next_turn_index: non_neg_integer()
        }
  def normalize_short_event(attrs, namespace, turn_index, now) do
    attrs_map = normalize_map(attrs)
    class = map_get(attrs_map, :class, :episodic)
    kind = map_get(attrs_map, :kind, :event)
    content = normalize_content(map_get(attrs_map, :content, %{}))
    text = normalize_text(map_get(attrs_map, :text), content)
    tags = normalize_tags(map_get(attrs_map, :tags, []))
    source = map_get(attrs_map, :source)

    chain_id =
      map_get(attrs_map, :chain_id) ||
        map_get(attrs_map, :conversation_id) ||
        get_in(attrs_map, [:metadata, "mem_os", "chain_id"]) ||
        get_in(attrs_map, [:metadata, :mem_os, :chain_id]) ||
        "chain:" <> namespace

    persona_keys =
      map_get(attrs_map, :persona_keys, tags_to_persona_keys(tags))
      |> normalize_tags()

    id =
      map_get(attrs_map, :id) ||
        short_event_id(%{
          namespace: namespace,
          chain_id: chain_id,
          class: class,
          kind: kind,
          text: text,
          content: content,
          tags: tags,
          source: source
        })

    normalized_attrs =
      attrs_map
      |> Map.put(:id, id)
      |> Map.put(:class, class)
      |> Map.put(:kind, kind)
      |> Map.put(:text, text)
      |> Map.put(:content, content)
      |> Map.put(:tags, tags)
      |> maybe_put(:source, source)
      |> Map.delete(:chain_id)
      |> Map.delete("chain_id")
      |> Map.delete(:conversation_id)
      |> Map.delete("conversation_id")
      |> Map.delete(:persona_keys)
      |> Map.delete("persona_keys")

    %{
      attrs: normalized_attrs,
      mem_os: %{
        tier: :short,
        chain_id: chain_id,
        heat: 1.0,
        promotion_score: 0.0,
        last_accessed_at: now,
        consolidation_version: 1,
        persona_keys: persona_keys
      },
      ingest_meta: %{
        "turn_index" => turn_index + 1,
        "chain_id" => chain_id,
        "ingested_at" => now
      },
      next_turn_index: turn_index + 1
    }
  end

  @doc """
  Groups records by chain id while preserving chronological order.
  """
  @spec group_by_chain([Record.t()], String.t()) :: %{String.t() => [Record.t()]}
  def group_by_chain(records, fallback_chain_id \\ "chain:default") do
    records
    |> Enum.group_by(fn record ->
      chain_id =
        case Metadata.from_record(record) do
          {:ok, mem_os} -> mem_os.chain_id
          _ -> nil
        end

      chain_id || fallback_chain_id
    end)
    |> Map.new(fn {chain_id, chain_records} ->
      sorted = Enum.sort_by(chain_records, &{&1.observed_at || 0, &1.id}, :asc)
      {chain_id, sorted}
    end)
  end

  @doc """
  Chunks records into segment groups constrained by count and token estimate.
  """
  @spec segment_records([Record.t()], pos_integer(), pos_integer()) :: [[Record.t()]]
  def segment_records(records, max_events, max_tokens) do
    {segments, current, _tokens} =
      Enum.reduce(records, {[], [], 0}, fn record, {segments, current, current_tokens} ->
        record_tokens = estimate_record_tokens(record)
        next_count = length(current) + 1
        next_tokens = current_tokens + record_tokens

        if current != [] and (next_count > max_events or next_tokens > max_tokens) do
          {[current | segments], [record], record_tokens}
        else
          {segments, current ++ [record], next_tokens}
        end
      end)

    segments =
      case current do
        [] -> segments
        _ -> [current | segments]
      end

    Enum.reverse(segments)
  end

  @doc """
  Builds attrs and mem_os metadata for a mid-tier segment record.
  """
  @spec build_mid_segment([Record.t()], String.t(), pos_integer(), integer(), integer()) :: map()
  def build_mid_segment(records, chain_id, ttl_ms, consolidation_version, now) do
    source_short_ids = Enum.map(records, & &1.id)
    segment_id = "segment_" <> stable_id({chain_id, source_short_ids})
    observed_at = records |> List.last() |> map_get(:observed_at, now)
    text = summarize_records(records, 900)
    tags = aggregate_tags(records) ++ ["memory_os:segment", "chain:" <> chain_id]
    persona_keys = aggregate_persona_keys(records)

    %{
      attrs: %{
        id: segment_id,
        class: :episodic,
        kind: :segment,
        text: text,
        content: %{
          chain_id: chain_id,
          source_short_ids: source_short_ids,
          event_count: length(records),
          events: Enum.map(records, &record_event_snapshot/1)
        },
        tags: Enum.uniq(tags),
        observed_at: observed_at,
        expires_at: observed_at + ttl_ms
      },
      mem_os: %{
        tier: :mid,
        chain_id: chain_id,
        segment_id: segment_id,
        heat: average_heat(records),
        promotion_score: 0.0,
        last_accessed_at: now,
        consolidation_version: consolidation_version,
        persona_keys: persona_keys
      },
      segment_id: segment_id,
      source_short_ids: source_short_ids
    }
  end

  @doc """
  Builds attrs and mem_os metadata for a mid-tier page record.
  """
  @spec build_mid_page([Record.t()], String.t(), pos_integer(), integer(), integer()) :: map()
  def build_mid_page(segment_records, chain_id, ttl_ms, consolidation_version, now) do
    segment_ids = Enum.map(segment_records, & &1.id)

    source_short_ids =
      segment_records |> Enum.flat_map(&segment_source_short_ids/1) |> Enum.uniq()

    page_id = "page_" <> stable_id({chain_id, segment_ids})
    observed_at = segment_records |> List.last() |> map_get(:observed_at, now)
    text = summarize_records(segment_records, 1_400)
    tags = aggregate_tags(segment_records) ++ ["memory_os:page", "chain:" <> chain_id]
    persona_keys = aggregate_persona_keys(segment_records)

    %{
      attrs: %{
        id: page_id,
        class: :semantic,
        kind: :page,
        text: text,
        content: %{
          chain_id: chain_id,
          segment_ids: segment_ids,
          source_short_ids: source_short_ids,
          segment_count: length(segment_records),
          promotion_candidate: true,
          time_range: %{
            from: hd(segment_records).observed_at,
            to: List.last(segment_records).observed_at
          }
        },
        tags: Enum.uniq(tags),
        observed_at: observed_at,
        expires_at: observed_at + ttl_ms
      },
      mem_os: %{
        tier: :mid,
        chain_id: chain_id,
        page_id: page_id,
        heat: average_heat(segment_records),
        promotion_score: 0.0,
        last_accessed_at: now,
        consolidation_version: consolidation_version,
        persona_keys: persona_keys
      },
      page_id: page_id,
      source_short_ids: source_short_ids
    }
  end

  @doc """
  Computes promotion score and eligibility for a mid-page candidate.
  """
  @spec promotion_score(Record.t(), non_neg_integer(), number()) :: %{
          score: number(),
          eligible?: boolean()
        }
  def promotion_score(page_record, recurrence_count, min_score) do
    {:ok, mem_os} = Metadata.from_record(page_record)
    heat_component = clamp(mem_os.heat, 0.0, 1.0)
    recurrence_component = clamp(recurrence_count / 3, 0.0, 1.0)
    persona_component = if length(mem_os.persona_keys) > 0, do: 1.0, else: 0.0

    score =
      Float.round(
        0.45 * heat_component + 0.4 * recurrence_component + 0.15 * persona_component,
        4
      )

    %{score: score, eligible?: score >= min_score}
  end

  @doc """
  Returns a normalized fact key for long-term conflict detection.
  """
  @spec fact_key(Record.t()) :: String.t()
  def fact_key(%Record{} = record) do
    from_tag =
      Enum.find_value(record.tags || [], fn
        "fact_key:" <> key -> key
        _ -> nil
      end)

    from_tag || stable_id(String.downcase(String.trim(record.text || inspect(record.content))))
  end

  @doc """
  Builds attrs and metadata patch for long-tier promotion.
  """
  @spec build_long_record(
          Record.t(),
          String.t(),
          number(),
          pos_integer(),
          integer(),
          integer(),
          atom(),
          [String.t()]
        ) :: map()
  def build_long_record(
        page_record,
        fact_key,
        score,
        ttl_ms,
        consolidation_version,
        now,
        conflict_strategy,
        previous_ids
      ) do
    {:ok, page_mem_os} = Metadata.from_record(page_record)
    long_id = "long_" <> stable_id({fact_key, page_record.id})
    observed_at = page_record.observed_at || now

    metadata_patch = %{
      "mem_os_conflict" => %{
        "reason" => Atom.to_string(conflict_strategy),
        "fact_key" => fact_key,
        "previous_ids" => previous_ids,
        "promoted_at" => now
      }
    }

    %{
      attrs: %{
        id: long_id,
        class: :semantic,
        kind: :fact,
        text: page_record.text,
        content: %{
          fact_key: fact_key,
          page_id: page_record.id,
          provenance: page_record.content
        },
        tags: Enum.uniq((page_record.tags || []) ++ ["memory_os:long", "fact_key:" <> fact_key]),
        metadata: metadata_patch,
        observed_at: observed_at,
        expires_at: observed_at + ttl_ms
      },
      mem_os: %{
        tier: :long,
        chain_id: page_mem_os.chain_id,
        page_id: page_mem_os.page_id || page_record.id,
        heat: page_mem_os.heat,
        promotion_score: score,
        last_accessed_at: now,
        consolidation_version: consolidation_version,
        persona_keys: page_mem_os.persona_keys
      }
    }
  end

  @doc """
  Produces attrs for re-writing an existing long record with updated metadata.
  """
  @spec rewrite_record_with_metadata(Record.t(), map()) :: map()
  def rewrite_record_with_metadata(%Record{} = record, metadata_patch) do
    merged_metadata =
      normalize_map(record.metadata)
      |> deep_merge(normalize_map(metadata_patch))

    %{
      id: record.id,
      class: record.class,
      kind: record.kind,
      text: record.text,
      content: record.content,
      tags: record.tags,
      source: record.source,
      observed_at: record.observed_at,
      expires_at: record.expires_at,
      embedding: record.embedding,
      metadata: merged_metadata,
      version: record.version
    }
  end

  @doc """
  Estimates token count for a record text/content.
  """
  @spec estimate_record_tokens(Record.t()) :: pos_integer()
  def estimate_record_tokens(%Record{} = record) do
    text =
      cond do
        is_binary(record.text) and record.text != "" -> record.text
        true -> inspect(record.content)
      end

    estimate_tokens(text)
  end

  @spec short_event_id(map()) :: String.t()
  defp short_event_id(payload), do: "short_" <> stable_id(payload)

  @spec stable_id(term()) :: String.t()
  defp stable_id(payload) do
    payload
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 24)
  end

  @spec normalize_text(term(), term()) :: String.t()
  defp normalize_text(text, _content) when is_binary(text) and text != "", do: text

  defp normalize_text(nil, content) when is_binary(content), do: content
  defp normalize_text(_, content), do: inspect(content)

  @spec normalize_content(term()) :: term()
  defp normalize_content(nil), do: %{}
  defp normalize_content(content), do: content

  @spec normalize_tags(term()) :: [String.t()]
  defp normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_tags(tags) when is_binary(tags), do: [String.trim(tags)]
  defp normalize_tags(_), do: []

  @spec tags_to_persona_keys([String.t()]) :: [String.t()]
  defp tags_to_persona_keys(tags) do
    tags
    |> Enum.filter(&String.starts_with?(&1, "persona:"))
    |> Enum.map(&String.replace_prefix(&1, "persona:", ""))
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  @spec aggregate_tags([Record.t()]) :: [String.t()]
  defp aggregate_tags(records) do
    records
    |> Enum.flat_map(&(&1.tags || []))
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  @spec aggregate_persona_keys([Record.t()]) :: [String.t()]
  defp aggregate_persona_keys(records) do
    records
    |> Enum.flat_map(fn record ->
      case Metadata.from_record(record) do
        {:ok, mem_os} -> mem_os.persona_keys
        _ -> []
      end
    end)
    |> Enum.uniq()
  end

  @spec average_heat([Record.t()]) :: number()
  defp average_heat([]), do: 0.0

  defp average_heat(records) do
    total =
      Enum.reduce(records, 0.0, fn record, acc ->
        heat =
          case Metadata.from_record(record) do
            {:ok, mem_os} -> mem_os.heat
            _ -> 0.0
          end

        acc + heat
      end)

    Float.round(total / length(records), 4)
  end

  @spec summarize_records([Record.t()], pos_integer()) :: String.t()
  defp summarize_records(records, max_chars) do
    records
    |> Enum.map(fn record ->
      cond do
        is_binary(record.text) and record.text != "" -> record.text
        true -> inspect(record.content)
      end
    end)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.join(" ")
    |> truncate(max_chars)
  end

  @spec truncate(String.t(), pos_integer()) :: String.t()
  defp truncate(text, max_chars) when is_binary(text) and byte_size(text) > max_chars do
    binary_part(text, 0, max_chars - 1) <> "…"
  end

  defp truncate(text, _max_chars), do: text

  @spec segment_source_short_ids(Record.t()) :: [String.t()]
  defp segment_source_short_ids(%Record{content: %{} = content}) do
    map_get(content, :source_short_ids, [])
    |> List.wrap()
    |> Enum.map(&to_string/1)
  end

  defp segment_source_short_ids(_record), do: []

  @spec record_event_snapshot(Record.t()) :: map()
  defp record_event_snapshot(%Record{} = record) do
    %{
      id: record.id,
      class: record.class,
      kind: record.kind,
      text: record.text,
      observed_at: record.observed_at,
      tags: record.tags
    }
  end

  @spec estimate_tokens(String.t()) :: pos_integer()
  defp estimate_tokens(text) when is_binary(text) do
    max(1, div(String.length(text), 4))
  end

  @spec clamp(number(), number(), number()) :: number()
  defp clamp(value, min_value, _max_value) when value < min_value, do: min_value
  defp clamp(value, _min_value, max_value) when value > max_value, do: max_value
  defp clamp(value, _min_value, _max_value), do: value

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec map_get(map(), atom() | String.t(), term()) :: term()
  defp map_get(map, key, default \\ nil)

  defp map_get(map, key, default) when is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp map_get(map, key, default) when is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        atom_key =
          Enum.find(Map.keys(map), fn
            entry when is_atom(entry) -> Atom.to_string(entry) == key
            _ -> false
          end)

        if atom_key, do: Map.get(map, atom_key, default), else: default
    end
  end

  @spec maybe_put(map(), atom(), term()) :: map()
  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  @spec deep_merge(map(), map()) :: map()
  defp deep_merge(left, right) do
    Map.merge(left, right, fn _key, left_value, right_value ->
      if is_map(left_value) and is_map(right_value) do
        deep_merge(left_value, right_value)
      else
        right_value
      end
    end)
  end
end

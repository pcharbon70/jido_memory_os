defmodule Jido.MemoryOS.DataSafety do
  @moduledoc """
  Retention, redaction, and retrieval masking helpers.
  """

  alias Jido.Memory.Record

  @default_retention %{
    short: %{max_ttl_ms: nil, blocked_tags: [], allowed_classes: []},
    mid: %{max_ttl_ms: nil, blocked_tags: [], allowed_classes: []},
    long: %{max_ttl_ms: nil, blocked_tags: [], allowed_classes: []}
  }

  @email_regex ~r/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}/
  @phone_regex ~r/\b(?:\+?1[\s\-]?)?(?:\(?\d{3}\)?[\s\-]?)\d{3}[\s\-]?\d{4}\b/
  @ssn_regex ~r/\b\d{3}-?\d{2}-?\d{4}\b/

  @doc """
  Applies retention rules and pre-persist redaction.
  """
  @spec sanitize_attrs(map(), :short | :mid | :long, map(), integer()) ::
          {:ok, map()} | {:error, term()}
  def sanitize_attrs(attrs, tier, config, now \\ System.system_time(:millisecond)) do
    with {:ok, retained} <- apply_retention(attrs, tier, config, now) do
      {:ok, redact_attrs(retained, map_get(config, :safety, %{}))}
    end
  end

  @doc """
  Applies role-based retrieval masking.
  """
  @spec mask_records([Record.t()], :allow | :mask | :drop) :: [Record.t()]
  def mask_records(records, :allow), do: records

  def mask_records(records, mode) do
    Enum.map(records, fn
      %Record{} = record ->
        text = mask_value(record.text, mode)
        content = mask_value(record.content, mode)
        metadata = mask_value(record.metadata, mode)
        source = mask_value(record.source, mode)

        %Record{record | text: text, content: content, metadata: metadata, source: source}

      record ->
        record
    end)
  end

  @doc """
  Applies masking to explain-retrieval payload maps.
  """
  @spec mask_explain_payload(map(), :allow | :mask | :drop) :: map()
  def mask_explain_payload(explain, :allow), do: explain

  def mask_explain_payload(explain, mode) do
    explain
    |> Map.update(:records, [], fn records ->
      records
      |> Enum.map(&mask_explain_record(&1, mode))
    end)
    |> Map.update(:context_pack, %{}, &mask_context_pack(&1, mode))
  end

  @doc """
  Hashes retrieval explanation payload for immutable audit entries.
  """
  @spec explanation_hash(map()) :: String.t()
  def explanation_hash(explain) when is_map(explain) do
    digest =
      explain
      |> Map.take([
        :query,
        :decision_trace,
        :scored_candidates,
        :selection_rationale,
        :result_count
      ])
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))

    Base.encode16(digest, case: :lower)
  end

  @spec apply_retention(map(), :short | :mid | :long, map(), integer()) ::
          {:ok, map()} | {:error, term()}
  defp apply_retention(attrs, tier, config, now) do
    retention = retention_policy(config, tier)
    class = normalize_class(map_get(attrs, :class))
    tags = normalize_tags(map_get(attrs, :tags, []))

    allowed_classes = normalize_class_list(map_get(retention, :allowed_classes, []))
    blocked_tags = normalize_tags(map_get(retention, :blocked_tags, []))

    with :ok <- enforce_allowed_class(class, allowed_classes),
         :ok <- enforce_blocked_tags(tags, blocked_tags) do
      {:ok, enforce_ttl(attrs, retention, now)}
    end
  end

  @spec retention_policy(map(), :short | :mid | :long) :: map()
  defp retention_policy(config, tier) do
    governance = map_get(config, :governance, %{})
    retention = map_get(governance, :retention, %{})

    @default_retention
    |> Map.fetch!(tier)
    |> Map.merge(normalize_map(map_get(retention, tier, %{})))
  end

  @spec enforce_allowed_class(String.t() | nil, [String.t()]) :: :ok | {:error, term()}
  defp enforce_allowed_class(_class, []), do: :ok
  defp enforce_allowed_class(nil, _allowed), do: {:error, {:retention_blocked, :class, nil}}

  defp enforce_allowed_class(class, allowed) do
    if class in allowed do
      :ok
    else
      {:error, {:retention_blocked, :class, class}}
    end
  end

  @spec enforce_blocked_tags([String.t()], [String.t()]) :: :ok | {:error, term()}
  defp enforce_blocked_tags(_tags, []), do: :ok

  defp enforce_blocked_tags(tags, blocked_tags) do
    case Enum.find(tags, &(&1 in blocked_tags)) do
      nil -> :ok
      tag -> {:error, {:retention_blocked, :tag, tag}}
    end
  end

  @spec enforce_ttl(map(), map(), integer()) :: map()
  defp enforce_ttl(attrs, retention, now) do
    case map_get(retention, :max_ttl_ms) do
      max_ttl_ms when is_integer(max_ttl_ms) and max_ttl_ms > 0 ->
        max_expires_at = now + max_ttl_ms
        existing_expires_at = normalize_integer(map_get(attrs, :expires_at))

        expires_at =
          if is_integer(existing_expires_at) do
            min(existing_expires_at, max_expires_at)
          else
            max_expires_at
          end

        Map.put(attrs, :expires_at, expires_at)

      _ ->
        attrs
    end
  end

  @spec redact_attrs(map(), map()) :: map()
  defp redact_attrs(attrs, safety_cfg) do
    redaction_enabled = map_get(safety_cfg, :redaction_enabled, true)
    strategy = normalize_mode(map_get(safety_cfg, :pii_strategy), :mask)

    if redaction_enabled and strategy != :allow do
      attrs
      |> Map.update(:text, nil, &mask_value(&1, strategy))
      |> Map.update(:content, %{}, &mask_value(&1, strategy))
      |> Map.update(:metadata, %{}, &mask_value(&1, strategy))
      |> Map.update(:source, nil, &mask_value(&1, strategy))
    else
      attrs
    end
  end

  @spec mask_explain_record(map(), :allow | :mask | :drop) :: map()
  defp mask_explain_record(record, mode) do
    record
    |> Map.update(:conflict, %{}, &mask_value(&1, mode))
  end

  @spec mask_context_pack(map(), :allow | :mask | :drop) :: map()
  defp mask_context_pack(context_pack, mode) do
    context_pack
    |> Map.update(:groups, [], fn groups ->
      Enum.map(groups, fn group ->
        Map.update(group, :entries, [], fn entries ->
          Enum.map(entries, fn entry ->
            entry
            |> Map.update(:text, nil, &mask_value(&1, mode))
            |> Map.update(:provenance, %{}, &mask_value(&1, mode))
          end)
        end)
      end)
    end)
    |> Map.update(:persona_hints, [], fn hints ->
      Enum.map(hints, fn hint ->
        hint
        |> Map.update(:hint, nil, &mask_value(&1, mode))
        |> Map.update(:source, %{}, &mask_value(&1, mode))
      end)
    end)
  end

  @spec mask_value(term(), :mask | :drop) :: term()
  defp mask_value(nil, _mode), do: nil

  defp mask_value(value, mode) when is_binary(value) do
    redacted =
      value
      |> then(&Regex.replace(@email_regex, &1, "[REDACTED_EMAIL]"))
      |> then(&Regex.replace(@phone_regex, &1, "[REDACTED_PHONE]"))
      |> then(&Regex.replace(@ssn_regex, &1, "[REDACTED_SSN]"))

    case mode do
      :drop ->
        if redacted == value, do: value, else: "[REDACTED]"

      :mask ->
        redacted
    end
  end

  defp mask_value(value, mode) when is_list(value), do: Enum.map(value, &mask_value(&1, mode))

  defp mask_value(value, mode) when is_map(value) do
    Enum.reduce(value, %{}, fn {key, item}, acc ->
      masked = mask_value(item, mode)

      cond do
        mode == :drop and masked == "[REDACTED]" ->
          acc

        true ->
          Map.put(acc, key, masked)
      end
    end)
  end

  defp mask_value(value, _mode), do: value

  @spec normalize_mode(term(), :allow | :mask | :drop) :: :allow | :mask | :drop
  defp normalize_mode(:allow, _fallback), do: :allow
  defp normalize_mode(:mask, _fallback), do: :mask
  defp normalize_mode(:drop, _fallback), do: :drop
  defp normalize_mode("allow", _fallback), do: :allow
  defp normalize_mode("mask", _fallback), do: :mask
  defp normalize_mode("drop", _fallback), do: :drop
  defp normalize_mode(_value, fallback), do: fallback

  @spec normalize_class(term()) :: String.t() | nil
  defp normalize_class(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalize_class()

  defp normalize_class(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_class(_), do: nil

  @spec normalize_class_list(term()) :: [String.t()]
  defp normalize_class_list(values) when is_list(values) do
    values
    |> Enum.map(&normalize_class/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalize_class_list(value), do: normalize_class_list([value])

  @spec normalize_tags(term()) :: [String.t()]
  defp normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(fn
      tag when is_binary(tag) -> String.trim(tag)
      tag when is_atom(tag) -> tag |> Atom.to_string() |> String.trim()
      tag -> tag |> to_string() |> String.trim()
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_tags(tag) when is_binary(tag), do: normalize_tags([tag])
  defp normalize_tags(tag) when is_atom(tag), do: normalize_tags([tag])
  defp normalize_tags(_tags), do: []

  @spec normalize_integer(term()) :: integer() | nil
  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> nil
    end
  end

  defp normalize_integer(_), do: nil

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default \\ nil) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end
end

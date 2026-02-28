defmodule Jido.MemoryOS.Metadata do
  @moduledoc """
  Encoder/decoder for `Record.metadata["mem_os"]`.
  """

  @tiers [:short, :mid, :long]

  @defaults %{
    tier: :short,
    chain_id: nil,
    segment_id: nil,
    page_id: nil,
    heat: 0.0,
    promotion_score: 0.0,
    last_accessed_at: nil,
    consolidation_version: 1,
    persona_keys: []
  }

  @allowed_tier_transitions %{
    short: [:short, :mid],
    mid: [:mid, :long],
    long: [:long]
  }

  @type t :: %{
          tier: :short | :mid | :long,
          chain_id: String.t() | nil,
          segment_id: String.t() | nil,
          page_id: String.t() | nil,
          heat: number(),
          promotion_score: number(),
          last_accessed_at: integer() | nil,
          consolidation_version: pos_integer(),
          persona_keys: [String.t()]
        }

  @doc """
  Returns metadata defaults for missing fields.
  """
  @spec defaults() :: t()
  def defaults, do: @defaults

  @doc """
  Encodes MemoryOS metadata into record attrs.
  """
  @spec encode_attrs(map() | keyword(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def encode_attrs(attrs, mem_os_updates, opts \\ []) do
    attrs_map = normalize_map(attrs)
    metadata = normalize_map(map_get(attrs_map, :metadata, %{}))

    with {:ok, existing} <- decode_metadata(metadata),
         updates <- normalize_patch_keys(mem_os_updates),
         {:ok, merged} <- normalize_mem_os_input(Map.merge(existing, updates)),
         :ok <- validate(merged),
         :ok <-
           validate_transition(
             Keyword.get(opts, :previous_tier, existing.tier),
             merged.tier,
             opts
           ) do
      mem_os_storage = to_storage_map(merged)

      merged_metadata =
        metadata
        |> Map.delete(:mem_os)
        |> Map.delete("mem_os")
        |> Map.put("mem_os", mem_os_storage)

      {:ok, Map.put(attrs_map, :metadata, merged_metadata)}
    end
  end

  @doc """
  Decodes and normalizes MemoryOS metadata from a metadata map.
  """
  @spec decode_metadata(map() | keyword() | nil) :: {:ok, t()} | {:error, term()}
  def decode_metadata(nil), do: {:ok, @defaults}

  def decode_metadata(metadata) do
    metadata_map = normalize_map(metadata)
    mem_os_raw = map_get(metadata_map, :mem_os)

    case mem_os_raw do
      nil -> {:ok, @defaults}
      value -> normalize_mem_os_input(value)
    end
  end

  @doc """
  Decodes metadata from a `Jido.Memory.Record`.
  """
  @spec from_record(Jido.Memory.Record.t()) :: {:ok, t()} | {:error, term()}
  def from_record(%Jido.Memory.Record{metadata: metadata}), do: decode_metadata(metadata)

  @doc """
  Validates normalized MemoryOS metadata.
  """
  @spec validate(map()) :: :ok | {:error, term()}
  def validate(mem_os) when is_map(mem_os) do
    with :ok <- validate_tier(mem_os.tier),
         :ok <- validate_optional_string(mem_os.chain_id, :chain_id),
         :ok <- validate_optional_string(mem_os.segment_id, :segment_id),
         :ok <- validate_optional_string(mem_os.page_id, :page_id),
         :ok <- validate_non_negative_number(mem_os.heat, :heat),
         :ok <- validate_non_negative_number(mem_os.promotion_score, :promotion_score),
         :ok <- validate_optional_integer(mem_os.last_accessed_at, :last_accessed_at),
         :ok <- validate_positive_integer(mem_os.consolidation_version, :consolidation_version),
         :ok <- validate_persona_keys(mem_os.persona_keys) do
      :ok
    end
  end

  @doc """
  Validates tier transitions across lifecycle operations.
  """
  @spec validate_transition(atom() | nil, atom(), keyword()) :: :ok | {:error, term()}
  def validate_transition(nil, new_tier, _opts), do: validate_tier(new_tier)

  def validate_transition(previous_tier, new_tier, opts) do
    strict = Keyword.get(opts, :strict_transition, true)

    if strict do
      allowed = Map.get(@allowed_tier_transitions, previous_tier, [])

      if new_tier in allowed do
        :ok
      else
        {:error, {:invalid_tier_transition, previous_tier, new_tier}}
      end
    else
      validate_tier(new_tier)
    end
  end

  @spec normalize_mem_os_input(map() | keyword()) :: {:ok, t()} | {:error, term()}
  defp normalize_mem_os_input(input) do
    map = normalize_map(input)

    tier = map_get(map, :tier, @defaults.tier)
    chain_id = normalize_optional_id(map_get(map, :chain_id, @defaults.chain_id))
    segment_id = normalize_optional_id(map_get(map, :segment_id, @defaults.segment_id))
    page_id = normalize_optional_id(map_get(map, :page_id, @defaults.page_id))
    heat = normalize_number(map_get(map, :heat, @defaults.heat), @defaults.heat)

    promotion_score =
      normalize_number(
        map_get(map, :promotion_score, @defaults.promotion_score),
        @defaults.promotion_score
      )

    last_accessed_at =
      normalize_optional_integer_value(
        map_get(map, :last_accessed_at, @defaults.last_accessed_at)
      )

    consolidation_version =
      normalize_positive_integer_value(
        map_get(map, :consolidation_version, @defaults.consolidation_version),
        @defaults.consolidation_version
      )

    persona_keys = normalize_persona_keys(map_get(map, :persona_keys, @defaults.persona_keys))

    normalized = %{
      tier: normalize_tier_value(tier, @defaults.tier),
      chain_id: chain_id,
      segment_id: segment_id,
      page_id: page_id,
      heat: heat,
      promotion_score: promotion_score,
      last_accessed_at: last_accessed_at,
      consolidation_version: consolidation_version,
      persona_keys: persona_keys
    }

    case validate(normalized) do
      :ok -> {:ok, normalized}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec to_storage_map(t()) :: map()
  defp to_storage_map(mem_os) do
    %{
      "tier" => Atom.to_string(mem_os.tier),
      "chain_id" => mem_os.chain_id,
      "segment_id" => mem_os.segment_id,
      "page_id" => mem_os.page_id,
      "heat" => mem_os.heat,
      "promotion_score" => mem_os.promotion_score,
      "last_accessed_at" => mem_os.last_accessed_at,
      "consolidation_version" => mem_os.consolidation_version,
      "persona_keys" => mem_os.persona_keys
    }
  end

  @spec validate_tier(term()) :: :ok | {:error, term()}
  defp validate_tier(tier) when tier in @tiers, do: :ok
  defp validate_tier(tier), do: {:error, {:invalid_mem_os_field, :tier, tier}}

  @spec validate_optional_string(term(), atom()) :: :ok | {:error, term()}
  defp validate_optional_string(nil, _field), do: :ok
  defp validate_optional_string(value, _field) when is_binary(value), do: :ok

  defp validate_optional_string(value, field),
    do: {:error, {:invalid_mem_os_field, field, value}}

  @spec validate_non_negative_number(term(), atom()) :: :ok | {:error, term()}
  defp validate_non_negative_number(value, _field) when is_number(value) and value >= 0, do: :ok

  defp validate_non_negative_number(value, field),
    do: {:error, {:invalid_mem_os_field, field, value}}

  @spec validate_optional_integer(term(), atom()) :: :ok | {:error, term()}
  defp validate_optional_integer(nil, _field), do: :ok
  defp validate_optional_integer(value, _field) when is_integer(value), do: :ok

  defp validate_optional_integer(value, field),
    do: {:error, {:invalid_mem_os_field, field, value}}

  @spec validate_positive_integer(term(), atom()) :: :ok | {:error, term()}
  defp validate_positive_integer(value, _field) when is_integer(value) and value > 0, do: :ok

  defp validate_positive_integer(value, field),
    do: {:error, {:invalid_mem_os_field, field, value}}

  @spec validate_persona_keys(term()) :: :ok | {:error, term()}
  defp validate_persona_keys(keys) when is_list(keys) do
    if Enum.all?(keys, &is_binary/1) do
      :ok
    else
      {:error, {:invalid_mem_os_field, :persona_keys, keys}}
    end
  end

  defp validate_persona_keys(value), do: {:error, {:invalid_mem_os_field, :persona_keys, value}}

  @spec normalize_tier_value(term(), atom()) :: atom()
  defp normalize_tier_value(value, fallback) do
    case value do
      tier when tier in @tiers -> tier
      "short" -> :short
      "mid" -> :mid
      "long" -> :long
      _ -> fallback
    end
  end

  @spec normalize_optional_id(term()) :: String.t() | nil
  defp normalize_optional_id(nil), do: nil

  defp normalize_optional_id(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_optional_id(value), do: to_string(value)

  @spec normalize_number(term(), number()) :: number()
  defp normalize_number(value, _fallback) when is_number(value) and value >= 0, do: value
  defp normalize_number(_value, fallback), do: fallback

  @spec normalize_optional_integer_value(term()) :: integer() | nil
  defp normalize_optional_integer_value(value) when is_integer(value), do: value
  defp normalize_optional_integer_value(_value), do: nil

  @spec normalize_positive_integer_value(term(), pos_integer()) :: pos_integer()
  defp normalize_positive_integer_value(value, _fallback) when is_integer(value) and value > 0,
    do: value

  defp normalize_positive_integer_value(_value, fallback), do: fallback

  @spec normalize_persona_keys(term()) :: [String.t()]
  defp normalize_persona_keys(keys) when is_list(keys) do
    keys
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp normalize_persona_keys(_keys), do: []

  @spec normalize_patch_keys(map() | keyword()) :: map()
  defp normalize_patch_keys(input) do
    input
    |> normalize_map()
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      case normalize_patch_key(key) do
        nil -> acc
        normalized_key -> Map.put(acc, normalized_key, value)
      end
    end)
  end

  @spec normalize_patch_key(term()) :: atom() | nil
  defp normalize_patch_key(:tier), do: :tier
  defp normalize_patch_key("tier"), do: :tier
  defp normalize_patch_key(:chain_id), do: :chain_id
  defp normalize_patch_key("chain_id"), do: :chain_id
  defp normalize_patch_key(:segment_id), do: :segment_id
  defp normalize_patch_key("segment_id"), do: :segment_id
  defp normalize_patch_key(:page_id), do: :page_id
  defp normalize_patch_key("page_id"), do: :page_id
  defp normalize_patch_key(:heat), do: :heat
  defp normalize_patch_key("heat"), do: :heat
  defp normalize_patch_key(:promotion_score), do: :promotion_score
  defp normalize_patch_key("promotion_score"), do: :promotion_score
  defp normalize_patch_key(:last_accessed_at), do: :last_accessed_at
  defp normalize_patch_key("last_accessed_at"), do: :last_accessed_at
  defp normalize_patch_key(:consolidation_version), do: :consolidation_version
  defp normalize_patch_key("consolidation_version"), do: :consolidation_version
  defp normalize_patch_key(:persona_keys), do: :persona_keys
  defp normalize_patch_key("persona_keys"), do: :persona_keys
  defp normalize_patch_key(_key), do: nil

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end

defmodule Jido.MemoryOS.Migration do
  @moduledoc """
  Phase 8 migration utility for moving legacy `jido_memory` data into MemoryOS.

  Features:
  - deterministic tier classification
  - `mem_os` metadata backfill
  - reconciliation reports for migration parity checks
  """

  alias Jido.Memory.Record
  alias Jido.Memory.Runtime
  alias Jido.MemoryOS.Metadata

  @tiers [:short, :mid, :long]
  @default_legacy_store {Jido.Memory.Store.ETS, [table: :jido_memory]}
  @default_legacy_limit 10_000

  @default_policy %{
    tag_tiers: %{
      "short_term" => :short,
      "mid_term" => :mid,
      "long_term" => :long
    },
    mid_text_length: 220,
    long_age_days: 30,
    short_expiry_ms: 3_600_000,
    promotion_scores: %{short: 0.2, mid: 0.5, long: 0.8}
  }

  @type tier :: :short | :mid | :long
  @type report :: %{
          target_id: String.t() | nil,
          legacy_namespace: String.t(),
          source_count: non_neg_integer(),
          migrated_count: non_neg_integer(),
          skipped_count: non_neg_integer(),
          failed_count: non_neg_integer(),
          per_tier: %{optional(tier()) => non_neg_integer()},
          failures: [map()],
          dry_run: boolean(),
          started_at: integer(),
          finished_at: integer()
        }

  @doc """
  Migrates one legacy namespace for a target into MemoryOS.

  ## Options
  - `:legacy_store` `{mod, opts}` store tuple for legacy records
  - `:legacy_namespace` explicit legacy namespace
  - `:legacy_namespace_template` template with `%{agent_id}` placeholder
  - `:legacy_limit` max records to read from legacy store
  - `:policy` classification policy map
  - `:server` MemoryOS manager server name
  - `:dry_run` when true, computes report without writing
  - `:on_error` `:continue` (default) or `:halt`
  """
  @spec migrate_target(map() | struct(), keyword()) :: {:ok, report()} | {:error, term()}
  def migrate_target(target, opts \\ []) do
    legacy_store = Keyword.get(opts, :legacy_store, @default_legacy_store)
    legacy_limit = positive_int(Keyword.get(opts, :legacy_limit), @default_legacy_limit)

    with {:ok, legacy_namespace} <- resolve_legacy_namespace(target, opts),
         {:ok, records} <-
           Runtime.recall(target, %{
             namespace: legacy_namespace,
             store: legacy_store,
             limit: legacy_limit,
             order: :asc
           }) do
      migrate_records(target, records,
        opts
        |> Keyword.put(:legacy_store, legacy_store)
        |> Keyword.put(:legacy_namespace, legacy_namespace)
      )
    end
  end

  @doc """
  Migrates an explicit record list and returns a deterministic report.
  """
  @spec migrate_records(map() | struct(), [Record.t() | map()], keyword()) ::
          {:ok, report()} | {:error, term()}
  def migrate_records(target, records, opts \\ []) when is_list(records) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    policy = normalize_policy(Keyword.get(opts, :policy, %{}), now)
    dry_run? = normalize_boolean(Keyword.get(opts, :dry_run, false), false)
    on_error = normalize_error_mode(Keyword.get(opts, :on_error, :continue))
    legacy_namespace = Keyword.get(opts, :legacy_namespace) || "agent:unknown"

    base_report = %{
      target_id: target_id(target),
      legacy_namespace: legacy_namespace,
      source_count: length(records),
      migrated_count: 0,
      skipped_count: 0,
      failed_count: 0,
      per_tier: %{short: 0, mid: 0, long: 0},
      failures: [],
      dry_run: dry_run?,
      started_at: now,
      finished_at: now
    }

    with {:ok, report} <-
           Enum.reduce_while(records, {:ok, base_report}, fn raw_record, {:ok, report} ->
             record_result = normalize_record(raw_record)

             case record_result do
               {:ok, record} ->
                 tier = classify_record(record, policy)

                 if dry_run? do
                   updated =
                     report
                     |> increment_tier(tier)
                     |> Map.update!(:migrated_count, &(&1 + 1))

                   {:cont, {:ok, updated}}
                 else
                   attrs =
                     backfill_attrs(record, tier,
                       policy: policy,
                       legacy_namespace: legacy_namespace,
                       now: now
                     )

                   case Jido.MemoryOS.remember(target, attrs, remember_opts(target, tier, opts)) do
                     {:ok, _stored} ->
                       updated =
                         report
                         |> increment_tier(tier)
                         |> Map.update!(:migrated_count, &(&1 + 1))

                       {:cont, {:ok, updated}}

                     {:error, reason} ->
                       failure = %{
                         id: record.id,
                         tier: tier,
                         error: reason
                       }

                       updated =
                         report
                         |> Map.update!(:failed_count, &(&1 + 1))
                         |> Map.update!(:failures, &[failure | &1])

                       if on_error == :halt do
                         {:halt, {:error, {:migration_failed, failure, updated}}}
                       else
                         {:cont, {:ok, updated}}
                       end
                   end
                 end

               {:error, reason} ->
                 failure = %{id: nil, tier: nil, error: reason}

                 updated =
                   report
                   |> Map.update!(:failed_count, &(&1 + 1))
                   |> Map.update!(:failures, &[failure | &1])

                 if on_error == :halt do
                   {:halt, {:error, {:invalid_legacy_record, reason, updated}}}
                 else
                   {:cont, {:ok, updated}}
                 end
             end
           end) do
      {:ok,
       report
       |> Map.update!(:failures, &Enum.reverse/1)
       |> Map.put(:finished_at, System.system_time(:millisecond))}
    end
  end

  @doc """
  Returns a reconciliation report between legacy and MemoryOS counts.
  """
  @spec reconcile_target(map() | struct(), keyword()) :: {:ok, map()} | {:error, term()}
  def reconcile_target(target, opts \\ []) do
    legacy_store = Keyword.get(opts, :legacy_store, @default_legacy_store)
    limit = positive_int(Keyword.get(opts, :limit), @default_legacy_limit)

    with {:ok, legacy_namespace} <- resolve_legacy_namespace(target, opts),
         {:ok, legacy_records} <-
           Runtime.recall(target, %{
             namespace: legacy_namespace,
             store: legacy_store,
             limit: limit,
             order: :asc
           }),
         {:ok, short} <- memory_os_retrieve(target, :short, limit, opts),
         {:ok, mid} <- memory_os_retrieve(target, :mid, limit, opts),
         {:ok, long} <- memory_os_retrieve(target, :long, limit, opts) do
      memory_count = dedupe_count(short ++ mid ++ long)

      {:ok,
       %{
         target_id: target_id(target),
         legacy_namespace: legacy_namespace,
         legacy_count: length(legacy_records),
         memory_os_count: memory_count,
         per_tier: %{short: length(short), mid: length(mid), long: length(long)},
         parity?: length(legacy_records) == memory_count,
         compared_at: System.system_time(:millisecond)
       }}
    end
  end

  @doc """
  Classifies one legacy record into a MemoryOS tier.
  """
  @spec classify_record(Record.t() | map(), map() | keyword()) :: tier()
  def classify_record(record, policy \\ %{}) do
    now = System.system_time(:millisecond)
    policy = normalize_policy(policy, now)

    with {:ok, normalized} <- normalize_record(record) do
      do_classify(normalized, policy)
    else
      _ -> :mid
    end
  end

  @doc """
  Backfills attributes for MemoryOS writes, including `metadata["mem_os"]`.
  """
  @spec backfill_attrs(Record.t(), tier(), keyword()) :: map()
  def backfill_attrs(%Record{} = record, tier, opts \\ []) when tier in @tiers do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    policy = normalize_policy(Keyword.get(opts, :policy, %{}), now)
    legacy_namespace = Keyword.get(opts, :legacy_namespace)

    base_attrs = %{
      id: record.id,
      class: record.class,
      kind: record.kind,
      text: record.text,
      content: record.content,
      tags: record.tags,
      source: record.source,
      observed_at: record.observed_at,
      expires_at: record.expires_at,
      metadata:
        normalize_map(record.metadata)
        |> Map.put("migration", %{
          "source" => "legacy_jido_memory",
          "migrated_at" => now,
          "legacy_namespace" => legacy_namespace,
          "classifier" => "phase8_default"
        })
    }

    mem_os_patch = %{
      tier: tier,
      chain_id: choose_chain_id(record, tier),
      heat: 0.0,
      promotion_score: Map.get(policy.promotion_scores, tier, 0.5),
      last_accessed_at: now,
      consolidation_version: 1,
      persona_keys: persona_keys(record)
    }

    case Metadata.encode_attrs(base_attrs, mem_os_patch, strict_transition: false) do
      {:ok, encoded} -> encoded
      {:error, _reason} -> base_attrs
    end
  end

  @spec do_classify(Record.t(), map()) :: tier()
  defp do_classify(record, policy) do
    metadata_tier = tier_from_metadata(record.metadata)

    cond do
      metadata_tier in @tiers ->
        metadata_tier

      tag_tier(record.tags, policy.tag_tiers) in @tiers ->
        tag_tier(record.tags, policy.tag_tiers)

      record.class == :semantic ->
        :long

      record.class == :working ->
        :short

      expires_soon?(record.expires_at, policy.now, policy.short_expiry_ms) ->
        :short

      older_than_days?(record.observed_at, policy.now, policy.long_age_days) ->
        :long

      text_size(record.text) >= policy.mid_text_length ->
        :mid

      true ->
        :mid
    end
  end

  @spec tier_from_metadata(term()) :: tier() | nil
  defp tier_from_metadata(metadata) do
    metadata_map = normalize_map(metadata)

    if Map.has_key?(metadata_map, :mem_os) or Map.has_key?(metadata_map, "mem_os") do
      case Metadata.decode_metadata(metadata_map) do
        {:ok, mem_os} when mem_os.tier in @tiers -> mem_os.tier
        _ -> nil
      end
    else
      nil
    end
  end

  @spec tag_tier([String.t()], map()) :: tier() | nil
  defp tag_tier(tags, tag_tiers) do
    tags
    |> Enum.map(&String.downcase/1)
    |> Enum.find_value(fn tag ->
      Map.get(tag_tiers, tag)
    end)
  end

  @spec expires_soon?(integer() | nil, integer(), pos_integer()) :: boolean()
  defp expires_soon?(nil, _now, _window), do: false
  defp expires_soon?(expires_at, now, window), do: expires_at - now <= window

  @spec older_than_days?(integer() | nil, integer(), pos_integer()) :: boolean()
  defp older_than_days?(nil, _now, _days), do: false

  defp older_than_days?(observed_at, now, days) do
    now - observed_at >= days * 86_400_000
  end

  @spec text_size(term()) :: non_neg_integer()
  defp text_size(text) when is_binary(text), do: String.length(text)
  defp text_size(_), do: 0

  @spec persona_keys(Record.t()) :: [String.t()]
  defp persona_keys(%Record{tags: tags}) do
    tags
    |> Enum.filter(&String.starts_with?(&1, "persona:"))
    |> Enum.map(&String.replace_prefix(&1, "persona:", ""))
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  @spec choose_chain_id(Record.t(), tier()) :: String.t()
  defp choose_chain_id(%Record{metadata: metadata, id: id}, tier) do
    meta = normalize_map(metadata)

    cond do
      is_binary(Map.get(meta, "chain_id")) ->
        Map.get(meta, "chain_id")

      is_binary(Map.get(meta, :chain_id)) ->
        Map.get(meta, :chain_id)

      true ->
        "legacy:migration:#{tier}:#{id}"
    end
  end

  @spec memory_os_retrieve(map() | struct(), tier(), pos_integer(), keyword()) ::
          {:ok, [Record.t()]} | {:error, term()}
  defp memory_os_retrieve(target, tier, limit, opts) do
    Jido.MemoryOS.retrieve(target, %{limit: limit},
      server: Keyword.get(opts, :server, Jido.MemoryOS.MemoryManager),
      tier: tier,
      tier_mode: tier,
      actor_id: Keyword.get(opts, :actor_id, target_id(target))
    )
  end

  @spec dedupe_count([Record.t()]) :: non_neg_integer()
  defp dedupe_count(records) do
    records
    |> Enum.map(& &1.id)
    |> Enum.uniq()
    |> length()
  end

  @spec remember_opts(map() | struct(), tier(), keyword()) :: keyword()
  defp remember_opts(target, tier, opts) do
    remember_opts = Keyword.get(opts, :remember_opts, [])

    base =
      opts
      |> Keyword.take([
        :server,
        :namespace,
        :store,
        :store_opts,
        :config,
        :app_config,
        :call_timeout,
        :timeout_ms,
        :approval_token,
        :agent_id
      ])
      |> Keyword.put(:tier, tier)
      |> Keyword.put_new(:actor_id, Keyword.get(opts, :actor_id, target_id(target)))

    Keyword.merge(base, remember_opts)
  end

  @spec resolve_legacy_namespace(map() | struct(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  defp resolve_legacy_namespace(target, opts) do
    case Keyword.get(opts, :legacy_namespace) do
      namespace when is_binary(namespace) and namespace != "" ->
        {:ok, namespace}

      _ ->
        case Keyword.get(opts, :legacy_namespace_template) do
          template when is_binary(template) and template != "" ->
            case target_id(target) do
              nil -> {:error, :namespace_required}
              agent_id -> {:ok, String.replace(template, "%{agent_id}", agent_id)}
            end

          _ ->
            case target_id(target) do
              nil -> {:error, :namespace_required}
              agent_id -> {:ok, "agent:" <> agent_id}
            end
        end
    end
  end

  @spec increment_tier(report(), tier()) :: report()
  defp increment_tier(report, tier) do
    update_in(report, [:per_tier, tier], fn current -> (current || 0) + 1 end)
  end

  @spec normalize_record(term()) :: {:ok, Record.t()} | {:error, term()}
  defp normalize_record(%Record{} = record), do: {:ok, record}

  defp normalize_record(%{} = map) do
    case Record.new(map, now: System.system_time(:millisecond)) do
      {:ok, record} -> {:ok, record}
      {:error, reason} -> {:error, {:invalid_legacy_record, reason}}
    end
  end

  defp normalize_record(other), do: {:error, {:invalid_legacy_record, other}}

  @spec normalize_policy(map() | keyword(), integer()) :: map()
  defp normalize_policy(policy, now) do
    input = normalize_map(policy)

    %{
      now: now,
      tag_tiers:
        normalize_tag_tiers(Map.get(input, :tag_tiers, Map.get(input, "tag_tiers", %{}))),
      mid_text_length:
        positive_int(
          Map.get(input, :mid_text_length, Map.get(input, "mid_text_length")),
          @default_policy.mid_text_length
        ),
      long_age_days:
        positive_int(
          Map.get(input, :long_age_days, Map.get(input, "long_age_days")),
          @default_policy.long_age_days
        ),
      short_expiry_ms:
        positive_int(
          Map.get(input, :short_expiry_ms, Map.get(input, "short_expiry_ms")),
          @default_policy.short_expiry_ms
        ),
      promotion_scores:
        normalize_promotion_scores(
          Map.get(input, :promotion_scores, Map.get(input, "promotion_scores", %{}))
        )
    }
  end

  @spec normalize_tag_tiers(term()) :: map()
  defp normalize_tag_tiers(value) when is_map(value) do
    merged = Map.merge(@default_policy.tag_tiers, normalize_map(value))

    Enum.reduce(merged, %{}, fn {key, tier}, acc ->
      normalized_tier = normalize_tier(tier)

      if normalized_tier in @tiers do
        Map.put(acc, normalize_string(key), normalized_tier)
      else
        acc
      end
    end)
  end

  defp normalize_tag_tiers(_), do: @default_policy.tag_tiers

  @spec normalize_promotion_scores(term()) :: %{short: number(), mid: number(), long: number()}
  defp normalize_promotion_scores(value) when is_map(value) do
    map = normalize_map(value)

    %{
      short: normalize_number(Map.get(map, :short, Map.get(map, "short")), 0.2),
      mid: normalize_number(Map.get(map, :mid, Map.get(map, "mid")), 0.5),
      long: normalize_number(Map.get(map, :long, Map.get(map, "long")), 0.8)
    }
  end

  defp normalize_promotion_scores(_), do: @default_policy.promotion_scores

  @spec normalize_tier(term()) :: tier() | nil
  defp normalize_tier(:short), do: :short
  defp normalize_tier(:mid), do: :mid
  defp normalize_tier(:long), do: :long
  defp normalize_tier("short"), do: :short
  defp normalize_tier("mid"), do: :mid
  defp normalize_tier("long"), do: :long
  defp normalize_tier(_), do: nil

  @spec normalize_error_mode(term()) :: :continue | :halt
  defp normalize_error_mode(:halt), do: :halt
  defp normalize_error_mode("halt"), do: :halt
  defp normalize_error_mode(_), do: :continue

  @spec positive_int(term(), pos_integer()) :: pos_integer()
  defp positive_int(value, _fallback) when is_integer(value) and value > 0, do: value
  defp positive_int(_value, fallback), do: fallback

  @spec normalize_number(term(), number()) :: number()
  defp normalize_number(value, _fallback) when is_number(value) and value >= 0, do: value
  defp normalize_number(_value, fallback), do: fallback

  @spec normalize_boolean(term(), boolean()) :: boolean()
  defp normalize_boolean(value, _fallback) when is_boolean(value), do: value
  defp normalize_boolean("true", _fallback), do: true
  defp normalize_boolean("false", _fallback), do: false
  defp normalize_boolean(_value, fallback), do: fallback

  @spec normalize_map(term()) :: map()
  defp normalize_map(%{} = map), do: map

  defp normalize_map(list) when is_list(list) do
    if Keyword.keyword?(list), do: Map.new(list), else: %{}
  end

  defp normalize_map(_), do: %{}

  @spec normalize_string(term()) :: String.t()
  defp normalize_string(value) when is_binary(value), do: String.downcase(String.trim(value))

  defp normalize_string(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalize_string()

  defp normalize_string(value), do: value |> to_string() |> normalize_string()

  @spec target_id(map() | struct()) :: String.t() | nil
  defp target_id(%{id: id}) when is_binary(id), do: id
  defp target_id(%{agent: %{id: id}}) when is_binary(id), do: id
  defp target_id(_), do: nil
end

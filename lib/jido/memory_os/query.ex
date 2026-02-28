defmodule Jido.MemoryOS.Query do
  @moduledoc """
  MemoryOS retrieval query contract used by planner/ranker/context-pack stages.
  """

  @type tier_mode :: :short | :mid | :long | :hybrid | :tiered | :long_priority | :all

  @type t :: %__MODULE__{
          tier_mode: tier_mode(),
          classes: [atom()],
          kinds: [term()],
          tags_any: [String.t()],
          tags_all: [String.t()],
          text_contains: String.t() | nil,
          since: integer() | nil,
          until: integer() | nil,
          limit: pos_integer(),
          order: :asc | :desc,
          persona_keys: [String.t()],
          topic_keys: [String.t()],
          candidate_fanout: pos_integer(),
          semantic_provider: module() | nil,
          semantic_timeout_ms: pos_integer(),
          context_token_budget: pos_integer(),
          include_excluded: boolean(),
          debug: boolean()
        }

  defstruct tier_mode: :hybrid,
            classes: [],
            kinds: [],
            tags_any: [],
            tags_all: [],
            text_contains: nil,
            since: nil,
            until: nil,
            limit: 20,
            order: :desc,
            persona_keys: [],
            topic_keys: [],
            candidate_fanout: 60,
            semantic_provider: nil,
            semantic_timeout_ms: 120,
            context_token_budget: 1_200,
            include_excluded: false,
            debug: false

  @doc """
  Normalizes query payload into MemoryOS query struct.
  """
  @spec new(map() | keyword() | Jido.Memory.Query.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(query, opts \\ []) do
    with {:ok, attrs} <- normalize_input(query),
         {:ok, tier_mode} <-
           normalize_tier_mode(resolve_attr(attrs, :tier_mode, opts[:tier_mode] || opts[:tier])),
         {:ok, order} <- normalize_order(resolve_attr(attrs, :order, :desc)),
         {:ok, limit} <-
           normalize_positive_integer(resolve_attr(attrs, :limit, opts[:default_limit] || 20), 20),
         {:ok, fanout} <-
           normalize_positive_integer(
             resolve_attr(attrs, :candidate_fanout, max(limit * 3, limit + 4)),
             max(limit * 3, limit + 4)
           ),
         {:ok, semantic_timeout_ms} <-
           normalize_positive_integer(resolve_attr(attrs, :semantic_timeout_ms, 120), 120),
         {:ok, context_token_budget} <-
           normalize_positive_integer(resolve_attr(attrs, :context_token_budget, 1_200), 1_200) do
      text_contains = normalize_text(resolve_attr(attrs, :text_contains))

      persona_keys =
        attrs
        |> resolve_attr(:persona_keys, [])
        |> normalize_tags()

      topic_keys =
        attrs
        |> resolve_attr(:topic_keys, infer_topic_keys(attrs))
        |> normalize_tags()

      semantic_provider = normalize_module(resolve_attr(attrs, :semantic_provider))

      query_struct = %__MODULE__{
        tier_mode: tier_mode,
        classes: normalize_atom_list(resolve_attr(attrs, :classes, [])),
        kinds: normalize_list(resolve_attr(attrs, :kinds, [])),
        tags_any: normalize_tags(resolve_attr(attrs, :tags_any, [])),
        tags_all: normalize_tags(resolve_attr(attrs, :tags_all, [])),
        text_contains: text_contains,
        since: normalize_optional_integer(resolve_attr(attrs, :since)),
        until: normalize_optional_integer(resolve_attr(attrs, :until)),
        limit: limit,
        order: order,
        persona_keys: persona_keys,
        topic_keys: topic_keys,
        candidate_fanout: fanout,
        semantic_provider: semantic_provider,
        semantic_timeout_ms: semantic_timeout_ms,
        context_token_budget: context_token_budget,
        include_excluded: normalize_boolean(resolve_attr(attrs, :include_excluded), false),
        debug: normalize_boolean(resolve_attr(attrs, :debug), false)
      }

      {:ok, query_struct}
    end
  end

  @doc """
  Produces runtime recall filters for a given tier fanout budget.
  """
  @spec to_runtime_filters(t(), pos_integer()) :: map()
  def to_runtime_filters(%__MODULE__{} = query, limit_override) do
    selective_tags =
      query.persona_keys
      |> Enum.map(&("persona:" <> &1))
      |> Kernel.++(Enum.map(query.topic_keys, &("topic:" <> &1)))

    %{
      classes: query.classes,
      kinds: query.kinds,
      tags_any: Enum.uniq(query.tags_any ++ selective_tags),
      tags_all: query.tags_all,
      text_contains: query.text_contains,
      since: query.since,
      until: query.until,
      limit: limit_override,
      order: query.order
    }
  end

  @spec normalize_input(map() | keyword() | Jido.Memory.Query.t()) ::
          {:ok, map()} | {:error, term()}
  defp normalize_input(%Jido.Memory.Query{} = query), do: {:ok, Map.from_struct(query)}
  defp normalize_input(%{} = query), do: {:ok, query}

  defp normalize_input(query) when is_list(query) do
    if Keyword.keyword?(query), do: {:ok, Map.new(query)}, else: {:error, :invalid_query}
  end

  defp normalize_input(_query), do: {:error, :invalid_query}

  @spec resolve_attr(map(), atom(), term()) :: term()
  defp resolve_attr(map, key, fallback \\ nil) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), fallback))
  end

  @spec normalize_tier_mode(term()) :: {:ok, tier_mode()} | {:error, term()}
  defp normalize_tier_mode(:short), do: {:ok, :short}
  defp normalize_tier_mode(:mid), do: {:ok, :mid}
  defp normalize_tier_mode(:long), do: {:ok, :long}
  defp normalize_tier_mode(:hybrid), do: {:ok, :hybrid}
  defp normalize_tier_mode(:tiered), do: {:ok, :tiered}
  defp normalize_tier_mode(:long_priority), do: {:ok, :long_priority}
  defp normalize_tier_mode(:all), do: {:ok, :all}
  defp normalize_tier_mode("short"), do: {:ok, :short}
  defp normalize_tier_mode("mid"), do: {:ok, :mid}
  defp normalize_tier_mode("long"), do: {:ok, :long}
  defp normalize_tier_mode("hybrid"), do: {:ok, :hybrid}
  defp normalize_tier_mode("tiered"), do: {:ok, :tiered}
  defp normalize_tier_mode("long_priority"), do: {:ok, :long_priority}
  defp normalize_tier_mode("all"), do: {:ok, :all}
  defp normalize_tier_mode(nil), do: {:ok, :hybrid}
  defp normalize_tier_mode(other), do: {:error, {:invalid_tier_mode, other}}

  @spec normalize_order(term()) :: {:ok, :asc | :desc} | {:error, term()}
  defp normalize_order(:asc), do: {:ok, :asc}
  defp normalize_order(:desc), do: {:ok, :desc}
  defp normalize_order("asc"), do: {:ok, :asc}
  defp normalize_order("desc"), do: {:ok, :desc}
  defp normalize_order(nil), do: {:ok, :desc}
  defp normalize_order(other), do: {:error, {:invalid_order, other}}

  @spec normalize_positive_integer(term(), pos_integer()) :: {:ok, pos_integer()}
  defp normalize_positive_integer(value, _fallback) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp normalize_positive_integer(_value, fallback), do: {:ok, fallback}

  @spec normalize_optional_integer(term()) :: integer() | nil
  defp normalize_optional_integer(value) when is_integer(value), do: value
  defp normalize_optional_integer(_value), do: nil

  @spec normalize_module(term()) :: module() | nil
  defp normalize_module(module) when is_atom(module), do: module
  defp normalize_module(_), do: nil

  @spec normalize_boolean(term(), boolean()) :: boolean()
  defp normalize_boolean(value, _fallback) when is_boolean(value), do: value
  defp normalize_boolean(_value, fallback), do: fallback

  @spec normalize_list(term()) :: list()
  defp normalize_list(list) when is_list(list), do: list
  defp normalize_list(_), do: []

  @spec normalize_atom_list(term()) :: [atom()]
  defp normalize_atom_list(list) when is_list(list) do
    list
    |> Enum.filter(&is_atom/1)
    |> Enum.uniq()
  end

  defp normalize_atom_list(_), do: []

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

  @spec normalize_text(term()) :: String.t() | nil
  defp normalize_text(text) when is_binary(text) do
    trimmed = String.trim(text)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_text(_), do: nil

  @spec infer_topic_keys(map()) :: [String.t()]
  defp infer_topic_keys(attrs) do
    resolve_attr(attrs, :tags_any, [])
    |> normalize_tags()
    |> Enum.filter(&String.starts_with?(&1, "topic:"))
    |> Enum.map(&String.replace_prefix(&1, "topic:", ""))
    |> Enum.reject(&(&1 == ""))
  end
end

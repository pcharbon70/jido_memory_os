defmodule Jido.MemoryOS.AccessPolicy do
  @moduledoc """
  Access policy evaluator for MemoryOS operations.

  Policy decisions are made from actor/group/action/tier scope and default to
  deny cross-agent access unless explicitly allowed.
  """

  @type action ::
          :remember
          | :retrieve
          | :explain_retrieval
          | :forget
          | :prune
          | :consolidate
          | :issue_approval_token
          | :policy_update

  @type context :: %{
          action: action(),
          tier: :short | :mid | :long,
          actor_id: String.t() | nil,
          actor_group: String.t() | nil,
          actor_role: String.t() | nil,
          target_agent_id: String.t() | nil,
          target_group: String.t() | nil,
          trace_id: String.t() | nil,
          correlation_id: String.t() | nil
        }

  @type decision :: %{
          allowed?: boolean(),
          reason: atom(),
          effect: :allow | :deny,
          matched_rule: map() | nil
        }

  @default_policy %{
    default_effect: :deny,
    allow_same_group: true,
    rules: []
  }

  @doc """
  Returns normalized policy defaults merged with user overrides.
  """
  @spec normalize_policy(map() | keyword() | nil) :: map()
  def normalize_policy(nil), do: @default_policy

  def normalize_policy(policy) do
    policy_map = normalize_map(policy)

    %{
      default_effect:
        normalize_effect(map_get(policy_map, :default_effect), @default_policy.default_effect),
      allow_same_group:
        normalize_boolean(
          map_get(policy_map, :allow_same_group),
          @default_policy.allow_same_group
        ),
      rules: normalize_rules(map_get(policy_map, :rules, []))
    }
  end

  @doc """
  Builds evaluation context from operation target and runtime options.
  """
  @spec context_from_request(action(), map() | struct(), keyword(), String.t() | nil) :: context()
  def context_from_request(action, target, runtime_opts, trace_id \\ nil) do
    target_map = normalize_map(target)
    tier = normalize_tier(Keyword.get(runtime_opts, :tier))

    actor_id =
      runtime_opts
      |> Keyword.get(:actor_id)
      |> normalize_optional_string()

    actor_group =
      runtime_opts
      |> Keyword.get(:actor_group)
      |> normalize_optional_string()

    actor_role =
      runtime_opts
      |> Keyword.get(:actor_role)
      |> normalize_optional_string()

    target_agent_id =
      runtime_opts
      |> Keyword.get(:agent_id)
      |> normalize_optional_string()
      |> case do
        nil ->
          map_get(target_map, :id)
          |> normalize_optional_string()

        value ->
          value
      end

    target_group =
      runtime_opts
      |> Keyword.get(:target_group)
      |> normalize_optional_string()
      |> case do
        nil ->
          map_get(target_map, :group)
          |> normalize_optional_string()

        value ->
          value
      end

    %{
      action: action,
      tier: tier,
      actor_id: actor_id || target_agent_id,
      actor_group: actor_group,
      actor_role: actor_role,
      target_agent_id: target_agent_id,
      target_group: target_group,
      trace_id: normalize_optional_string(trace_id),
      correlation_id: normalize_optional_string(Keyword.get(runtime_opts, :correlation_id))
    }
  end

  @doc """
  Evaluates one policy decision.
  """
  @spec evaluate(context(), map() | keyword() | nil) :: decision()
  def evaluate(context, policy_input) do
    policy = normalize_policy(policy_input)

    same_actor? =
      present_and_equal?(context.actor_id, context.target_agent_id) or
        is_nil(context.target_agent_id)

    same_group? = present_and_equal?(context.actor_group, context.target_group)
    cross_agent? = not same_actor?

    case match_rule(context, policy.rules, cross_agent?) do
      {:allow, rule} ->
        %{allowed?: true, reason: :rule_allow, effect: :allow, matched_rule: rule}

      {:deny, rule} ->
        %{allowed?: false, reason: :rule_deny, effect: :deny, matched_rule: rule}

      :no_match ->
        cond do
          same_actor? ->
            %{allowed?: true, reason: :same_actor, effect: :allow, matched_rule: nil}

          same_group? and policy.allow_same_group ->
            %{allowed?: true, reason: :same_group, effect: :allow, matched_rule: nil}

          policy.default_effect == :allow ->
            %{allowed?: true, reason: :policy_default_allow, effect: :allow, matched_rule: nil}

          true ->
            reason = if cross_agent?, do: :cross_agent_denied, else: :policy_default_deny
            %{allowed?: false, reason: reason, effect: :deny, matched_rule: nil}
        end
    end
  end

  @doc """
  Derives retrieval masking mode from actor role.
  """
  @spec masking_mode(context(), map() | keyword() | nil) :: :allow | :mask | :drop
  def masking_mode(context, governance_input) do
    governance = normalize_map(governance_input)
    masking = normalize_map(map_get(governance, :masking, %{}))
    role_modes = normalize_role_modes(map_get(masking, :role_modes, %{}))

    default_mode = normalize_mode(map_get(masking, :default_mode), :allow)

    case context.actor_role do
      nil ->
        default_mode

      role ->
        Map.get(role_modes, role, default_mode)
    end
  end

  @spec match_rule(context(), [map()], boolean()) :: {:allow | :deny, map()} | :no_match
  defp match_rule(_context, [], _cross_agent?), do: :no_match

  defp match_rule(context, [rule | rest], cross_agent?) do
    if rule_matches?(context, rule, cross_agent?) do
      {normalize_effect(map_get(rule, :effect), :deny), rule}
    else
      match_rule(context, rest, cross_agent?)
    end
  end

  @spec rule_matches?(context(), map(), boolean()) :: boolean()
  defp rule_matches?(context, rule, cross_agent?) do
    actor_ok? = scope_matches?(context.actor_id, map_get(rule, :actors))
    group_ok? = scope_matches?(context.actor_group, map_get(rule, :groups))
    role_ok? = scope_matches?(context.actor_role, map_get(rule, :roles))
    action_ok? = action_matches?(context.action, map_get(rule, :actions))
    tier_ok? = tier_matches?(context.tier, map_get(rule, :tiers))
    target_ok? = scope_matches?(context.target_agent_id, map_get(rule, :targets))

    cross_agent_ok? =
      case map_get(rule, :cross_agent) do
        nil -> true
        value -> normalize_boolean(value, cross_agent?) == cross_agent?
      end

    actor_ok? and group_ok? and role_ok? and action_ok? and tier_ok? and target_ok? and
      cross_agent_ok?
  end

  @spec action_matches?(action(), term()) :: boolean()
  defp action_matches?(_action, nil), do: true

  defp action_matches?(action, actions) do
    normalize_actions(actions)
    |> Enum.any?(fn
      :* -> true
      item -> item == action
    end)
  end

  @spec tier_matches?(:short | :mid | :long, term()) :: boolean()
  defp tier_matches?(_tier, nil), do: true

  defp tier_matches?(tier, tiers) do
    normalize_tiers(tiers)
    |> Enum.any?(fn
      :* -> true
      item -> item == tier
    end)
  end

  @spec scope_matches?(String.t() | nil, term()) :: boolean()
  defp scope_matches?(_value, nil), do: true

  defp scope_matches?(value, scope) do
    values = normalize_strings(scope)

    cond do
      values == [] ->
        true

      "*" in values ->
        true

      is_nil(value) ->
        false

      true ->
        value in values
    end
  end

  @spec normalize_rules(term()) :: [map()]
  defp normalize_rules(rules) when is_list(rules) do
    rules
    |> Enum.map(&normalize_map/1)
    |> Enum.filter(&(map_size(&1) > 0))
    |> Enum.map(fn rule ->
      %{
        effect: normalize_effect(map_get(rule, :effect), :deny),
        actors: normalize_strings(map_get(rule, :actors)),
        groups: normalize_strings(map_get(rule, :groups)),
        roles: normalize_strings(map_get(rule, :roles)),
        actions: normalize_actions(map_get(rule, :actions)),
        tiers: normalize_tiers(map_get(rule, :tiers)),
        targets: normalize_strings(map_get(rule, :targets)),
        cross_agent: map_get(rule, :cross_agent)
      }
    end)
  end

  defp normalize_rules(_rules), do: []

  @spec normalize_role_modes(term()) :: map()
  defp normalize_role_modes(%{} = role_modes) do
    Enum.reduce(role_modes, %{}, fn {key, value}, acc ->
      role =
        case key do
          atom when is_atom(atom) -> Atom.to_string(atom)
          binary when is_binary(binary) -> String.trim(binary)
          other -> to_string(other)
        end

      if role == "" do
        acc
      else
        Map.put(acc, role, normalize_mode(value, :mask))
      end
    end)
  end

  defp normalize_role_modes(_), do: %{}

  @spec normalize_effect(term(), :allow | :deny) :: :allow | :deny
  defp normalize_effect(:allow, _fallback), do: :allow
  defp normalize_effect(:deny, _fallback), do: :deny
  defp normalize_effect("allow", _fallback), do: :allow
  defp normalize_effect("deny", _fallback), do: :deny
  defp normalize_effect(_value, fallback), do: fallback

  @spec normalize_mode(term(), :allow | :mask | :drop) :: :allow | :mask | :drop
  defp normalize_mode(:allow, _fallback), do: :allow
  defp normalize_mode(:mask, _fallback), do: :mask
  defp normalize_mode(:drop, _fallback), do: :drop
  defp normalize_mode("allow", _fallback), do: :allow
  defp normalize_mode("mask", _fallback), do: :mask
  defp normalize_mode("drop", _fallback), do: :drop
  defp normalize_mode(_value, fallback), do: fallback

  @spec normalize_tier(term()) :: :short | :mid | :long
  defp normalize_tier(:short), do: :short
  defp normalize_tier(:mid), do: :mid
  defp normalize_tier(:long), do: :long
  defp normalize_tier("short"), do: :short
  defp normalize_tier("mid"), do: :mid
  defp normalize_tier("long"), do: :long
  defp normalize_tier(_tier), do: :short

  @spec present_and_equal?(String.t() | nil, String.t() | nil) :: boolean()
  defp present_and_equal?(left, right) when is_binary(left) and is_binary(right),
    do: left == right

  defp present_and_equal?(_left, _right), do: false

  @spec normalize_actions(term()) :: [atom()]
  defp normalize_actions(values) when is_list(values) do
    values
    |> Enum.map(&normalize_action/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalize_actions(value), do: normalize_actions([value])

  @spec normalize_action(term()) :: atom() | nil
  defp normalize_action(:*), do: :*
  defp normalize_action(:remember), do: :remember
  defp normalize_action(:retrieve), do: :retrieve
  defp normalize_action(:explain_retrieval), do: :explain_retrieval
  defp normalize_action(:forget), do: :forget
  defp normalize_action(:prune), do: :prune
  defp normalize_action(:consolidate), do: :consolidate
  defp normalize_action(:issue_approval_token), do: :issue_approval_token
  defp normalize_action(:policy_update), do: :policy_update

  defp normalize_action(binary) when is_binary(binary) do
    trimmed = String.trim(binary)

    case trimmed do
      "" -> nil
      "*" -> :*
      "remember" -> :remember
      "retrieve" -> :retrieve
      "explain_retrieval" -> :explain_retrieval
      "forget" -> :forget
      "prune" -> :prune
      "consolidate" -> :consolidate
      "issue_approval_token" -> :issue_approval_token
      "policy_update" -> :policy_update
      _ -> nil
    end
  end

  defp normalize_action(_value), do: nil

  @spec normalize_tiers(term()) :: [atom()]
  defp normalize_tiers(values) when is_list(values) do
    values
    |> Enum.map(&normalize_tier_rule/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalize_tiers(value), do: normalize_tiers([value])

  @spec normalize_tier_rule(term()) :: atom() | nil
  defp normalize_tier_rule(:*), do: :*
  defp normalize_tier_rule(:short), do: :short
  defp normalize_tier_rule(:mid), do: :mid
  defp normalize_tier_rule(:long), do: :long

  defp normalize_tier_rule(value) when is_binary(value) do
    case String.trim(value) do
      "*" -> :*
      "short" -> :short
      "mid" -> :mid
      "long" -> :long
      _ -> nil
    end
  end

  defp normalize_tier_rule(_value), do: nil

  @spec normalize_strings(term()) :: [String.t()]
  defp normalize_strings(values) when is_list(values) do
    values
    |> Enum.map(&normalize_optional_string/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalize_strings(value), do: normalize_strings([value])

  @spec normalize_optional_string(term()) :: String.t() | nil
  defp normalize_optional_string(nil), do: nil

  defp normalize_optional_string(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_optional_string(value) when is_atom(value) do
    value |> Atom.to_string() |> normalize_optional_string()
  end

  defp normalize_optional_string(value), do: value |> to_string() |> normalize_optional_string()

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

  defp normalize_map(_value), do: %{}

  @spec map_get(map(), atom(), term()) :: term()
  defp map_get(map, key, default \\ nil) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end
end

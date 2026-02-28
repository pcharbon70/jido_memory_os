defmodule Jido.MemoryOS.ApprovalToken do
  @moduledoc """
  Approval token lifecycle helpers for gated operations.
  """

  @type token_entry :: %{
          token: String.t(),
          actor_id: String.t() | nil,
          actions: [atom()],
          issued_at: integer(),
          expires_at: integer(),
          one_time: boolean(),
          reason: String.t() | nil
        }

  @doc """
  Issues an approval token and stores it in the token map.
  """
  @spec issue(map(), keyword(), keyword()) :: {:ok, token_entry(), map()}
  def issue(tokens, opts, defaults \\ []) do
    now = System.system_time(:millisecond)
    ttl_ms = positive_int(Keyword.get(opts, :ttl_ms), Keyword.get(defaults, :ttl_ms, 300_000))

    one_time =
      normalize_boolean(Keyword.get(opts, :one_time), Keyword.get(defaults, :one_time, true))

    actor_id = normalize_optional_string(Keyword.get(opts, :actor_id))

    actions =
      Keyword.get(opts, :actions, Keyword.get(defaults, :actions, []))
      |> normalize_actions()

    reason = normalize_optional_string(Keyword.get(opts, :reason))
    token = generate_token()

    entry = %{
      token: token,
      actor_id: actor_id,
      actions: actions,
      issued_at: now,
      expires_at: now + ttl_ms,
      one_time: one_time,
      reason: reason
    }

    max_tokens = positive_int(Keyword.get(defaults, :max_tokens), 500)

    next_tokens =
      tokens
      |> cleanup_expired(now)
      |> Map.put(token, entry)
      |> trim_to_limit(max_tokens)

    {:ok, entry, next_tokens}
  end

  @doc """
  Validates an approval token for one action and actor.
  """
  @spec validate(map(), String.t() | nil, atom(), String.t() | nil) ::
          {:ok, token_entry(), map()} | {:error, term(), map()}
  def validate(tokens, token, action, actor_id) do
    now = System.system_time(:millisecond)
    cleaned = cleanup_expired(tokens, now)

    with token when is_binary(token) <- token,
         {:ok, entry} <- fetch_entry(cleaned, token),
         :ok <- check_actor(entry, actor_id),
         :ok <- check_action(entry, action),
         :ok <- check_not_expired(entry, now) do
      next_tokens =
        if entry.one_time do
          Map.delete(cleaned, token)
        else
          cleaned
        end

      {:ok, entry, next_tokens}
    else
      nil -> {:error, :approval_token_required, cleaned}
      {:error, reason} -> {:error, reason, cleaned}
      _ -> {:error, :approval_token_invalid, cleaned}
    end
  end

  @doc """
  Removes expired tokens.
  """
  @spec cleanup_expired(map(), integer()) :: map()
  def cleanup_expired(tokens, now \\ System.system_time(:millisecond)) do
    Enum.reduce(tokens, %{}, fn {token, entry}, acc ->
      if entry.expires_at > now do
        Map.put(acc, token, entry)
      else
        acc
      end
    end)
  end

  @spec fetch_entry(map(), String.t()) :: {:ok, token_entry()} | {:error, term()}
  defp fetch_entry(tokens, token) do
    case Map.get(tokens, token) do
      %{} = entry -> {:ok, entry}
      _ -> {:error, :approval_token_invalid}
    end
  end

  @spec check_actor(token_entry(), String.t() | nil) :: :ok | {:error, term()}
  defp check_actor(%{actor_id: nil}, _actor_id), do: :ok
  defp check_actor(%{actor_id: actor_id}, actor_id), do: :ok
  defp check_actor(_entry, _actor_id), do: {:error, :approval_token_actor_mismatch}

  @spec check_action(token_entry(), atom()) :: :ok | {:error, term()}
  defp check_action(%{actions: []}, _action), do: :ok

  defp check_action(%{actions: actions}, action) do
    if action in actions do
      :ok
    else
      {:error, :approval_token_action_not_allowed}
    end
  end

  @spec check_not_expired(token_entry(), integer()) :: :ok | {:error, term()}
  defp check_not_expired(%{expires_at: expires_at}, now) do
    if expires_at > now, do: :ok, else: {:error, :approval_token_expired}
  end

  @spec trim_to_limit(map(), pos_integer()) :: map()
  defp trim_to_limit(tokens, max_tokens) do
    if map_size(tokens) <= max_tokens do
      tokens
    else
      tokens
      |> Map.values()
      |> Enum.sort_by(& &1.issued_at, :desc)
      |> Enum.take(max_tokens)
      |> Map.new(fn entry -> {entry.token, entry} end)
    end
  end

  @spec generate_token() :: String.t()
  defp generate_token do
    "apr-" <> Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)
  end

  @spec normalize_actions(term()) :: [atom()]
  defp normalize_actions(actions) when is_list(actions) do
    actions
    |> Enum.map(&normalize_action/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp normalize_actions(action), do: normalize_actions([action])

  @spec normalize_action(term()) :: atom() | nil
  defp normalize_action(action) when is_atom(action) do
    if action in [:forget, :overwrite, :policy_update], do: action, else: nil
  end

  defp normalize_action(action) when is_binary(action) do
    trimmed = String.trim(action)

    if trimmed == "" do
      nil
    else
      case trimmed do
        "forget" -> :forget
        "overwrite" -> :overwrite
        "policy_update" -> :policy_update
        _ -> nil
      end
    end
  end

  defp normalize_action(_), do: nil

  @spec positive_int(term(), pos_integer()) :: pos_integer()
  defp positive_int(value, _fallback) when is_integer(value) and value > 0, do: value
  defp positive_int(_value, fallback), do: fallback

  @spec normalize_optional_string(term()) :: String.t() | nil
  defp normalize_optional_string(nil), do: nil

  defp normalize_optional_string(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_optional_string(value), do: value |> to_string() |> normalize_optional_string()

  @spec normalize_boolean(term(), boolean()) :: boolean()
  defp normalize_boolean(value, _fallback) when is_boolean(value), do: value
  defp normalize_boolean("true", _fallback), do: true
  defp normalize_boolean("false", _fallback), do: false
  defp normalize_boolean(_value, fallback), do: fallback
end

defmodule Jido.MemoryOS.Journal do
  @moduledoc """
  Lightweight append-only operation journal used for replay-safe recovery.

  Journal events are persisted as one Erlang-term payload per line.
  """

  @type event :: map()

  @spec load(String.t() | nil) :: {:ok, [event()]} | {:error, term()}
  def load(nil), do: {:ok, []}

  def load(path) when is_binary(path) do
    if File.exists?(path) do
      path
      |> File.stream!([], :line)
      |> Enum.reduce_while({:ok, []}, fn line, {:ok, acc} ->
        with {:ok, event} <- decode_line(line) do
          {:cont, {:ok, [event | acc]}}
        else
          {:error, _reason} -> {:cont, {:ok, acc}}
        end
      end)
      |> case do
        {:ok, events} -> {:ok, Enum.reverse(events)}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, []}
    end
  end

  @spec append(String.t() | nil, event()) :: :ok | {:error, term()}
  def append(nil, _event), do: :ok

  def append(path, event) when is_binary(path) and is_map(event) do
    encoded = encode_line(event)

    with :ok <- ensure_parent(path),
         :ok <- File.write(path, encoded, [:append]) do
      :ok
    end
  end

  @spec compact(String.t() | nil, [event()]) :: :ok | {:error, term()}
  def compact(nil, _events), do: :ok

  def compact(path, events) when is_binary(path) and is_list(events) do
    payload = Enum.map_join(events, "", &encode_line/1)

    with :ok <- ensure_parent(path),
         :ok <- File.write(path, payload, [:write]) do
      :ok
    end
  end

  @spec build_index([event()]) :: %{optional(String.t()) => event()}
  def build_index(events) when is_list(events) do
    Enum.reduce(events, %{}, fn event, acc ->
      case event_request_id(event) do
        request_id when is_binary(request_id) and request_id != "" ->
          Map.put(acc, request_id, event)

        _ ->
          acc
      end
    end)
  end

  @spec event_request_id(event()) :: String.t() | nil
  def event_request_id(event) do
    Map.get(event, :request_id, Map.get(event, "request_id"))
  end

  @spec decode_line(binary()) :: {:ok, event()} | {:error, term()}
  defp decode_line(line) do
    line = String.trim(line)

    if line == "" do
      {:ok, %{}}
    else
      with {:ok, payload} <- Base.decode64(line) do
        try do
          decoded = :erlang.binary_to_term(payload, [:safe])

          if is_map(decoded) do
            {:ok, decoded}
          else
            {:error, :invalid_journal_event}
          end
        rescue
          ArgumentError -> {:error, :invalid_journal_term}
        end
      else
        :error -> {:error, :invalid_journal_encoding}
        error -> {:error, error}
      end
    end
  end

  @spec encode_line(event()) :: iodata()
  defp encode_line(event) do
    event
    |> :erlang.term_to_binary()
    |> Base.encode64()
    |> Kernel.<>("\n")
  end

  @spec ensure_parent(String.t()) :: :ok | {:error, term()}
  defp ensure_parent(path) do
    path
    |> Path.dirname()
    |> File.mkdir_p()
  end
end

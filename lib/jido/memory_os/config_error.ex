defmodule Jido.MemoryOS.ConfigError do
  @moduledoc """
  Structured config validation error with field-path context.
  """

  @enforce_keys [:path, :code, :message]
  defstruct [:path, :code, :message, :value]

  @type t :: %__MODULE__{
          path: [atom() | String.t()],
          code: atom(),
          message: String.t(),
          value: term()
        }

  @spec new([atom() | String.t()], atom(), String.t(), term()) :: t()
  def new(path, code, message, value \\ nil) do
    %__MODULE__{path: path, code: code, message: message, value: value}
  end
end

defmodule Jido.MemoryOS.Compatibility do
  @moduledoc """
  Dependency compatibility checks for `jido_memory`.

  Phase 1 keeps MemoryOS pinned to `jido_memory 0.1.x` and validates runtime
  capabilities so breakage is caught early.
  """

  @runtime_module Jido.Memory.Runtime
  @version_requirement "~> 0.1.0"
  @required_runtime_functions [
    remember: 3,
    get: 3,
    recall: 2,
    forget: 3,
    prune_expired: 2,
    resolve_runtime: 3
  ]

  case Code.ensure_compiled(@runtime_module) do
    {:module, _module} ->
      :ok

    {:error, _reason} ->
      raise CompileError,
        description: "missing required dependency module #{inspect(@runtime_module)}"
  end

  for {function_name, arity} <- @required_runtime_functions do
    if not function_exported?(@runtime_module, function_name, arity) do
      raise CompileError,
        description:
          "missing #{inspect(@runtime_module)}.#{function_name}/#{arity} required by Jido.MemoryOS"
    end
  end

  @doc """
  Returns the required `jido_memory` version constraint.
  """
  @spec version_requirement() :: String.t()
  def version_requirement, do: @version_requirement

  @doc """
  Returns required runtime function capabilities.
  """
  @spec required_runtime_functions() :: keyword(pos_integer())
  def required_runtime_functions, do: @required_runtime_functions

  @doc """
  Validates both `jido_memory` version and required runtime functions.
  """
  @spec validate_runtime_contract() :: :ok | {:error, term()}
  def validate_runtime_contract do
    with {:ok, _version} <- validate_jido_memory_version(),
         :ok <- validate_required_capabilities(@runtime_module) do
      :ok
    end
  end

  @doc """
  Validates only required functions on a runtime module.
  """
  @spec validate_required_capabilities(module()) :: :ok | {:error, term()}
  def validate_required_capabilities(module) when is_atom(module) do
    missing =
      @required_runtime_functions
      |> Enum.reject(fn {function_name, arity} ->
        function_exported?(module, function_name, arity)
      end)

    if missing == [] do
      :ok
    else
      {:error, {:missing_runtime_capabilities, module, missing}}
    end
  end

  @doc """
  Validates loaded `jido_memory` application version against supported range.
  """
  @spec validate_jido_memory_version() :: {:ok, String.t()} | {:error, term()}
  def validate_jido_memory_version do
    case Application.spec(:jido_memory, :vsn) do
      nil ->
        {:error, :jido_memory_not_loaded}

      version when is_list(version) ->
        validate_version_string(to_string(version))

      version when is_binary(version) ->
        validate_version_string(version)

      version ->
        {:error, {:invalid_jido_memory_version, version}}
    end
  end

  @spec validate_version_string(String.t()) :: {:ok, String.t()} | {:error, term()}
  defp validate_version_string(version) do
    if Version.match?(version, @version_requirement) do
      {:ok, version}
    else
      {:error, {:unsupported_jido_memory_version, version, @version_requirement}}
    end
  end
end

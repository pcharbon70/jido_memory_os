defmodule Jido.MemoryOS.ErrorMapping do
  @moduledoc """
  Maps MemoryOS/internal reasons to native `Jido.Error` structs.
  """

  alias Jido.MemoryOS.ConfigError

  @type jido_error ::
          Jido.Error.ValidationError.t()
          | Jido.Error.ExecutionError.t()
          | Jido.Error.RoutingError.t()
          | Jido.Error.TimeoutError.t()
          | Jido.Error.CompensationError.t()
          | Jido.Error.InternalError.t()

  @doc """
  Converts arbitrary reason terms into typed Jido errors.
  """
  @spec from_reason(term(), atom()) :: jido_error()
  def from_reason(reason, operation) do
    case reason do
      {:error, nested} ->
        from_reason(nested, operation)

      :namespace_required ->
        Jido.Error.validation_error("namespace could not be resolved",
          kind: :input,
          subject: :namespace,
          details: details(operation, reason, %{code: :namespace_required})
        )

      :invalid_query ->
        Jido.Error.validation_error("query payload is invalid",
          kind: :input,
          subject: :query,
          details: details(operation, reason, %{code: :invalid_query})
        )

      :invalid_id ->
        Jido.Error.validation_error("record id is invalid",
          kind: :input,
          subject: :id,
          details: details(operation, reason, %{code: :invalid_id})
        )

      {:invalid_tier, tier} ->
        Jido.Error.validation_error("invalid memory tier",
          kind: :input,
          subject: :tier,
          details: details(operation, reason, %{code: :invalid_tier, tier: tier})
        )

      :not_found ->
        Jido.Error.execution_error("memory record not found",
          phase: :execution,
          details: details(operation, reason, %{code: :not_found})
        )

      {:missing_runtime_capabilities, module, missing} ->
        Jido.Error.validation_error("jido_memory runtime capabilities are missing",
          kind: :config,
          subject: module,
          details:
            details(operation, reason, %{
              code: :runtime_incompatible,
              missing_capabilities: missing
            })
        )

      {:unsupported_jido_memory_version, version, requirement} ->
        Jido.Error.validation_error("unsupported jido_memory version",
          kind: :config,
          subject: :jido_memory_version,
          details:
            details(operation, reason, %{
              code: :runtime_incompatible,
              version: version,
              requirement: requirement
            })
        )

      {:invalid_tier_transition, previous_tier, next_tier} ->
        Jido.Error.validation_error("invalid tier transition in metadata",
          kind: :input,
          subject: :tier,
          details:
            details(operation, reason, %{
              code: :invalid_lifecycle_transition,
              previous_tier: previous_tier,
              next_tier: next_tier
            })
        )

      {:invalid_mem_os_field, field, value} ->
        Jido.Error.validation_error("invalid MemoryOS metadata field",
          kind: :input,
          subject: field,
          details:
            details(operation, reason, %{code: :invalid_metadata, field: field, value: value})
        )

      {:runtime_exception, exception, stacktrace} ->
        Jido.Error.internal_error("memory runtime raised an exception",
          details:
            details(operation, reason, %{
              code: :runtime_exception,
              exception: inspect(exception),
              stacktrace: stacktrace
            })
        )

      errors when is_list(errors) ->
        if config_errors?(errors) do
          Jido.Error.validation_error("invalid MemoryOS configuration",
            kind: :config,
            subject: :config,
            details: details(operation, reason, %{code: :invalid_config, errors: errors})
          )
        else
          Jido.Error.execution_error("memory runtime returned error list",
            phase: :execution,
            details: details(operation, reason, %{code: :upstream_error_list, errors: errors})
          )
        end

      _ ->
        Jido.Error.execution_error("upstream memory runtime error",
          phase: :execution,
          details: details(operation, reason, %{code: :upstream_error})
        )
    end
  end

  @spec config_errors?([term()]) :: boolean()
  defp config_errors?(errors), do: Enum.all?(errors, &match?(%ConfigError{}, &1))

  @spec details(atom(), term(), map()) :: map()
  defp details(operation, reason, extra) do
    extra
    |> Map.put_new(:operation, operation)
    |> Map.put_new(:reason, reason)
  end
end

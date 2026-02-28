defmodule Jido.MemoryOS.Retrieval.SemanticProvider do
  @moduledoc """
  Behavior for semantic scoring providers used by the hybrid ranker.
  """

  alias Jido.MemoryOS.Query

  @type score_map :: %{optional(String.t()) => number()}

  @callback score(Query.t(), [map()], keyword()) :: {:ok, score_map()} | {:error, term()}
end

"""Use text2sql as middleware in a LangChain agent.

This shows how to plug text2sql into any LangChain agent built with
`create_agent`. The middleware injects:
  - `execute_sql` tool (read-only SQL execution)
  - `lookup_example` tool (optional — when examples= is set)
  - Dialect-aware schema-exploration guidance in the system prompt

Install:
    pip install "text2sql[langchain,anthropic]"
"""

from langchain.agents import create_agent

from text2sql import Text2SqlMiddleware


def main() -> None:
    t2s = Text2SqlMiddleware(
        db_url="sqlite:///mydb.db",
        # Optional: inject domain knowledge from a markdown file
        # examples="scenarios.md",
    )

    agent = create_agent(
        model="anthropic:claude-sonnet-4-6",
        tools=t2s.tools,
        middleware=[t2s],
    )

    result = agent.invoke(
        {"messages": [{"role": "user", "content": "What are the top 5 customers by revenue?"}]}
    )

    print(result["messages"][-1].content)


if __name__ == "__main__":
    main()

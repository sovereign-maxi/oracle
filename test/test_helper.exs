ExUnit.start()

# Start Faker
{:ok, _} = Application.ensure_all_started(:faker)

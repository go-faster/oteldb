// Define an environment named "dev"
// Run as:
//  atlas migrate --env dev diff some-migration-name
env "dev" {
  // Define the URL of the Dev Database for this environment
  // See: https://atlasgo.io/concepts/dev-database
  dev = "docker://postgres/14/test?search_path=public"

  // Local URL for the database. Only if you want to experiment locally,
  // not required for migration generation.
  //
  // Run with docker (make pg_up):
  //  docker run --name otelbench-pg -e POSTGRES_DB=otelbench -e POSTGRES_PASSWORD=otelbench -e POSTGRES_USER=otelbench -p 127.0.0.1:5432:5432 -d postgres:14
  // Stop and remove (make pg_down):
  //  docker rm -f otelbench-pg
  // Connect (make pg_connect):
  //  psql postgres://otelbench:otelbench@localhost:5432/otelbench
  // Migrate (make pg_migrate):
  //  atlas migrate apply --env dev
  url = "postgres://otelbench:otelbench@localhost:5432/otelbench?search_path=public&sslmode=disable"

  src = "ent://ent/schema"
}

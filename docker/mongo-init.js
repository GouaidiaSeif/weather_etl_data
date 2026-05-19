// Creates the application database user on first MongoDB startup.
// Root credentials come from MONGO_INITDB_ROOT_USERNAME / MONGO_INITDB_ROOT_PASSWORD.

db = db.getSiblingDB("weather_etl");

db.createUser({
  user: "weather_user",
  pwd: "weather_pass",
  roles: [{ role: "readWrite", db: "weather_etl" }],
});

print("Created weather_user with readWrite on weather_etl");

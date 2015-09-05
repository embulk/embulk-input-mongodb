Embulk::JavaPlugin.register_input(
  "mongodb", "org.embulk.input.mongodb.MongodbInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))

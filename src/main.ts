app.enableCors({
  origin: [
    "http://localhost:3000",
    "http://localhost:3002",
    "https://the1970-admin-5747vbo51-the1970-cos-projects.vercel.app",
    "https://operations.the1970.co",
  ],
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: true,
});
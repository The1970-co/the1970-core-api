import "dotenv/config";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors({
    origin: [
      "http://localhost:3000",
      "http://localhost:3002",
      "the1970-admin-5747vbo51-the1970-cos-projects.vercel.app",
      "https://operations.the1970.co",
    ],
    credentials: true,
  });

  await app.listen(process.env.PORT ?? 3001);
}

bootstrap();
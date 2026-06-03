import "dotenv/config";
import { Logger } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";

const responseLogger = new Logger("ResponseMeter");

function toPositiveNumber(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function chunkSize(chunk: any, encoding?: BufferEncoding) {
  if (!chunk) return 0;
  if (Buffer.isBuffer(chunk)) return chunk.length;
  if (typeof chunk === "string") return Buffer.byteLength(chunk, encoding || "utf8");
  try {
    return Buffer.byteLength(JSON.stringify(chunk), "utf8");
  } catch {
    return 0;
  }
}

function installLargeResponseMeter(app: any) {
  const enabled = process.env.LARGE_RESPONSE_LOG_ENABLED !== "false";
  if (!enabled) return;

  const minBytes = toPositiveNumber(
    process.env.LARGE_RESPONSE_MIN_BYTES,
    200 * 1024,
  );
  const minDurationMs = toPositiveNumber(
    process.env.SLOW_RESPONSE_MIN_MS,
    1000,
  );

  app.use((req: any, res: any, next: any) => {
    const startedAt = Date.now();
    let responseBytes = 0;

    const originalWrite = res.write.bind(res);
    const originalEnd = res.end.bind(res);

    res.write = (chunk: any, encoding?: BufferEncoding, callback?: any) => {
      responseBytes += chunkSize(chunk, encoding);
      return originalWrite(chunk, encoding as any, callback);
    };

    res.end = (chunk?: any, encoding?: BufferEncoding, callback?: any) => {
      responseBytes += chunkSize(chunk, encoding);
      return originalEnd(chunk, encoding as any, callback);
    };

    res.on("finish", () => {
      const durationMs = Date.now() - startedAt;
      const isLarge = responseBytes >= minBytes;
      const isSlow = durationMs >= minDurationMs;

      if (!isLarge && !isSlow) return;
      if (req.method === "OPTIONS") return;

      const sizeKb = Math.round(responseBytes / 1024);
      const sizeMb = (responseBytes / 1024 / 1024).toFixed(2);
      const label = isLarge ? "LARGE_RESPONSE" : "SLOW_RESPONSE";

      responseLogger.warn(
        `[${label}] ${req.method} ${req.originalUrl || req.url} status=${res.statusCode} size=${sizeKb}KB/${sizeMb}MB duration=${durationMs}ms`,
      );
    });

    next();
  });
}

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors({
    origin: [
      "http://localhost:3000",
      "http://localhost:3002",
      "https://the1970-admin-5747vbo51-the1970-cos-projects.vercel.app",
      "https://operations.the1970.co",
    ],
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: [
      "Content-Type",
      "Authorization",
      "x-active-branch-id",
      "x-branch-id",
    ],
    credentials: true,
  });

  installLargeResponseMeter(app);

  await app.listen(process.env.PORT ?? 3001);
}

bootstrap();

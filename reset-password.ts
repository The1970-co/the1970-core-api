import * as dotenv from 'dotenv';
import { PrismaClient } from '@prisma/client';
import * as bcrypt from 'bcrypt';

dotenv.config();

if (!process.env.DATABASE_URL) {
  throw new Error('Thiếu DATABASE_URL trong file .env');
}

const prisma = new PrismaClient();

async function main() {
  const code = 'NV001';
  const password = '123456ABC';

  const passwordHash = await bcrypt.hash(password, 10);

  const user = await prisma.staffUser.update({
    where: { code },
    data: {
      passwordHash,
      isActive: true,
    },
  });

  console.log(`✅ Reset password thành công cho ${user.code}`);
}

main()
  .catch((e) => {
    console.error('❌ Lỗi reset password:', e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
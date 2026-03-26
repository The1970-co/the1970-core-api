import 'dotenv/config'
import { PrismaClient } from '@prisma/client'
import * as bcrypt from 'bcrypt'

const prisma = new PrismaClient()

async function main() {
  const existing = await prisma.adminUser.findUnique({
    where: { email: 'owner@the1970.vn' },
  })

  if (existing) {
    console.log('Owner already exists')
    return
  }

  const passwordHash = await bcrypt.hash('123456', 10)

  await prisma.adminUser.create({
    data: {
      fullName: 'Owner The 1970',
      email: 'owner@the1970.vn',
      passwordHash,
      role: 'OWNER',
      isActive: true,
    },
  })

  console.log('Seed done')
}

main()
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
  .finally(async () => {
    await prisma.$disconnect()
  })
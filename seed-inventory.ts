import 'dotenv/config';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const BRANCHES = ['b1', 'b2', 'b3'];

async function main() {
  const variants = await prisma.productVariant.findMany({
    select: { id: true, sku: true },
  });

  for (const variant of variants) {
    for (const branchId of BRANCHES) {
      await prisma.inventoryItem.upsert({
        where: {
          variantId_branchId: {
            variantId: variant.id,
            branchId,
          },
        },
        update: {},
        create: {
          variantId: variant.id,
          branchId,
          availableQty: 0,
          reservedQty: 0,
          incomingQty: 0,
        },
      });
    }
  }

  console.log(`✅ Seeded inventory rows for ${variants.length} variants x ${BRANCHES.length} branches`);
}

main()
  .catch(console.error)
  .finally(async () => {
    await prisma.$disconnect();
  });
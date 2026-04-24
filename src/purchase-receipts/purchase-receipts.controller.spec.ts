import { Test, TestingModule } from '@nestjs/testing';
import { PurchaseReceiptsController } from './purchase-receipts.controller';

describe('PurchaseReceiptsController', () => {
  let controller: PurchaseReceiptsController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [PurchaseReceiptsController],
    }).compile();

    controller = module.get<PurchaseReceiptsController>(PurchaseReceiptsController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});

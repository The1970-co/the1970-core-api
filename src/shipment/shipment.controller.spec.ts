import { Test, TestingModule } from '@nestjs/testing';
import { ShipmentController } from './shipment.controller';

describe('ShipmentController', () => {
  let controller: ShipmentController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ShipmentController],
    }).compile();

    controller = module.get<ShipmentController>(ShipmentController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});

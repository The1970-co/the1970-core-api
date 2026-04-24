import { Injectable } from "@nestjs/common";

@Injectable()
export class MobileProfileService {
  async getProfile(user: any) {
    return {
      id: user?.id ?? null,
      code: user?.code ?? null,
      name: user?.name ?? user?.fullName ?? null,
      role: user?.role ?? null,
      branchId: user?.branchId ?? "all",
      branchName: user?.branchName ?? "Tất cả chi nhánh",
      isActive: user?.isActive ?? true,
    };
  }
}
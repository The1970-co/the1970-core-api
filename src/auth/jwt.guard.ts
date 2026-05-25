import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as jwt from "jsonwebtoken";

@Injectable()
export class JwtGuard implements CanActivate {
  constructor(private readonly prisma: PrismaService) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    const authorization = req.headers.authorization as string | undefined;

    if (!authorization || !authorization.startsWith("Bearer ")) {
      throw new UnauthorizedException("Missing token");
    }

    const token = authorization.replace("Bearer ", "").trim();

    let payload: any;
    try {
      payload = jwt.verify(token, process.env.JWT_SECRET || "dev-secret");
    } catch {
      throw new UnauthorizedException("Invalid token");
    }

    if (payload.type !== "access" || !payload.sub || !payload.sid) {
      throw new UnauthorizedException("Invalid token");
    }

    const session = await this.prisma.staffSession.findUnique({
      where: { id: String(payload.sid) },
      include: {
        staff: {
          include: {
            roles: true,
            branchRoles: { include: { branch: true } },
            branchPermissions: { include: { branch: true } },
          },
        },
      },
    });

    if (!session || session.revokedAt || session.expiresAt <= new Date()) {
      throw new UnauthorizedException("Phiên đăng nhập đã hết hạn.");
    }

    if (!session.staff || !session.staff.isActive) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    if (Number(payload.sv || 0) !== Number(session.staff.sessionVersion || 1)) {
      throw new UnauthorizedException("Quyền đã thay đổi, vui lòng đăng nhập lại.");
    }

    const roles = Array.from(
      new Set(
        [
          ...(Array.isArray(session.staff.roles)
            ? session.staff.roles.map((r: any) => r.roleCode)
            : []),
          session.staff.role,
        ]
          .map((r) => String(r || "").toLowerCase())
          .filter(Boolean),
      ),
    );

    const normalizeBranchId = (value: any) => String(value || "").trim();

    const branchIds = Array.from(
      new Set(
        [
          session.staff.branchId,
          ...(session.staff.branchRoles || []).map((row: any) => row?.branchId || row?.branch?.id),
          ...(session.staff.branchPermissions || []).map((row: any) => row?.branchId || row?.branch?.id),
        ]
          .map(normalizeBranchId)
          .filter(Boolean),
      ),
    );

    const requestedBranchId = normalizeBranchId(
      req.headers["x-active-branch-id"] ||
        req.headers["x-branch-id"] ||
        req.headers["active-branch-id"],
    );

    const activeBranchId =
      requestedBranchId && branchIds.includes(requestedBranchId)
        ? requestedBranchId
        : normalizeBranchId(session.staff.branchId) || branchIds[0] || "";

    const isOwnerOrAdmin = roles.includes("owner") || roles.includes("admin");
    const permissionKeys = isOwnerOrAdmin
      ? ["*"]
      : (() => {
          const keys = new Set<string>();
          const denied = new Set<string>();

          const addKeys = (values: any[]) => {
            if (!Array.isArray(values)) return;
            values
              .map((key: any) => String(key || "").trim())
              .filter(Boolean)
              .forEach((key: string) => keys.add(key));
          };

          const removeKeys = (values: any[]) => {
            if (!Array.isArray(values)) return;
            values
              .map((key: any) => String(key || "").trim())
              .filter(Boolean)
              .forEach((key: string) => denied.add(key));
          };

          const rows = activeBranchId
            ? (session.staff.branchPermissions || []).filter(
                (row: any) => normalizeBranchId(row?.branchId || row?.branch?.id) === activeBranchId,
              )
            : session.staff.branchPermissions || [];

          const rowsToUse = rows.length ? rows : session.staff.branchPermissions || [];

          rowsToUse.forEach((row: any) => {
            addKeys(row?.permissionKeys);
            addKeys(row?.extraPermissionKeys);
            removeKeys(row?.deniedPermissionKeys);
          });

          denied.forEach((key) => keys.delete(key));

          return Array.from(keys);
        })();

    const branchOptions = branchIds.map((branchId) => {
      const roleRow = (session.staff.branchRoles || []).find(
        (row: any) => normalizeBranchId(row?.branchId || row?.branch?.id) === branchId,
      );
      const permissionRow = (session.staff.branchPermissions || []).find(
        (row: any) => normalizeBranchId(row?.branchId || row?.branch?.id) === branchId,
      );

      return {
        branchId,
        branchName:
          roleRow?.branch?.name ||
          permissionRow?.branch?.name ||
          (normalizeBranchId(session.staff.branchId) === branchId ? session.staff.branchName : "") ||
          branchId,
        role: String(roleRow?.roleCode || session.staff.role || "").toLowerCase(),
      };
    });

    req.user = {
      id: session.staff.id,
      sub: session.staff.id,
      sessionId: session.id,
      sessionVersion: session.staff.sessionVersion || 1,
      code: session.staff.code,
      name: session.staff.name,
      role: String(session.staff.role || "").toLowerCase(),
      roles,
      branchId: session.staff.branchId,
      branchName: session.staff.branchName,
      activeBranchId,
      branchIds,
      branchOptions,
      branchRoles: session.staff.branchRoles || [],
      branchPermissions: session.staff.branchPermissions || [],
      permissions: permissionKeys,
      type: "staff",
      lastLoginAt: session.staff.lastLoginAt,
      status: session.staff.isActive ? "active" : "inactive",
    };

    return true;
  }
}

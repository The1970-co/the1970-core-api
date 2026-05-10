import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { REQUIRED_PERMISSIONS_KEY } from "../decorators/require-permissions.decorator";

@Injectable()
export class PermissionGuard implements CanActivate {
  constructor(private readonly reflector: Reflector) {}

  canActivate(context: ExecutionContext): boolean {
    const required =
      this.reflector.getAllAndOverride<string[]>(REQUIRED_PERMISSIONS_KEY, [
        context.getHandler(),
        context.getClass(),
      ]) || [];

    if (!required.length) return true;

    const req = context.switchToHttp().getRequest();
    const user = req.user;
    const permissions = Array.isArray(user?.permissions)
      ? user.permissions.map((item: any) => String(item || "").trim())
      : [];

    if (permissions.includes("*")) return true;

    const ok = required.every((permission) => permissions.includes(permission));
    if (!ok) throw new ForbiddenException("Bạn không có quyền thực hiện thao tác này");

    return true;
  }
}

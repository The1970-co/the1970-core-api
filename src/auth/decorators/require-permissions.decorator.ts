import { applyDecorators, SetMetadata } from "@nestjs/common";

export const REQUIRED_PERMISSIONS_KEY = "required_permissions";
export const REQUIRED_PERMISSION_MODE_KEY = "required_permission_mode";
export type RequiredPermissionMode = "all" | "any";

export function RequirePermissions(...permissions: string[]) {
  return applyDecorators(
    SetMetadata(REQUIRED_PERMISSIONS_KEY, permissions),
    SetMetadata(REQUIRED_PERMISSION_MODE_KEY, "all" as RequiredPermissionMode),
  );
}

export function RequireAnyPermissions(...permissions: string[]) {
  return applyDecorators(
    SetMetadata(REQUIRED_PERMISSIONS_KEY, permissions),
    SetMetadata(REQUIRED_PERMISSION_MODE_KEY, "any" as RequiredPermissionMode),
  );
}

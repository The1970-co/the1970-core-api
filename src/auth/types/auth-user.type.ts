export type AuthUser = {
  id: string;
  sub: string;
  sessionId: string;
  sessionVersion: number;
  code?: string;
  name?: string;
  role?: string;
  roles: string[];
  branchId?: string | null;
  branchName?: string | null;
  branchRoles: any[];
  branchPermissions: any[];
  permissions: string[];
  type: "staff";
};

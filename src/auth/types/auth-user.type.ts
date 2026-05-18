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
  activeBranchId?: string | null;
  branchIds?: string[];
  branchOptions?: Array<{
    branchId: string;
    branchName: string;
    role?: string;
  }>;
  branchRoles: any[];
  branchPermissions: any[];
  permissions: string[];
  type: "staff";
};

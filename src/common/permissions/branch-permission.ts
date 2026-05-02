export function hasBranchPermission(
  user: any,
  branchId: string,
  key: string
) {
  const p = user.branchPermissions?.find(
    (b: any) => String(b.branchId) === String(branchId)
  );

  return Boolean(p?.[key]);
}
from sqlalchemy import exists, or_, select
from app.models.dimension import RoleOrgJoin, StoreModel


def build_store_permission_query(role_code: str):
    """
    构建带数据权限控制的店铺查询

    Args:
        role_code: 角色代码

    Returns:
        带权限过滤的 StoreModel 查询，只包含 store_code 和 store_name 列
    """
    return (
        select(
            StoreModel.store_code,
            StoreModel.store_name,
            StoreModel.store_type
        )
            # level_1 权限检查 (manage_channel)
            .where(
            exists().where(
                RoleOrgJoin.org_level == 'level_1',
                RoleOrgJoin.org_level_value == StoreModel.manage_channel,
                RoleOrgJoin.role_code == role_code
            )
        )
            # level_2 权限检查 (manage_region)
            .where(
            or_(
                exists().where(
                    RoleOrgJoin.org_level == 'level_2',
                    RoleOrgJoin.org_level_value == StoreModel.manage_region,
                    RoleOrgJoin.role_code == role_code
                ),
                ~exists().where(
                    RoleOrgJoin.org_level == 'level_2',
                    RoleOrgJoin.role_code == role_code
                )
            )
        )
            # level_3 权限检查 (City)
            .where(
            or_(
                exists().where(
                    RoleOrgJoin.org_level == 'level_3',
                    RoleOrgJoin.org_level_value == StoreModel.City,
                    RoleOrgJoin.role_code == role_code
                ),
                ~exists().where(
                    RoleOrgJoin.org_level == 'level_3',
                    RoleOrgJoin.role_code == role_code
                )
            )
        )
            # level_4 权限检查 (store_code)
            .where(
            or_(
                exists().where(
                    RoleOrgJoin.org_level == 'level_4',
                    RoleOrgJoin.org_level_value == StoreModel.store_code,
                    RoleOrgJoin.role_code == role_code
                ),
                ~exists().where(
                    RoleOrgJoin.org_level == 'level_4',
                    RoleOrgJoin.role_code == role_code
                )
            )
        )
    )

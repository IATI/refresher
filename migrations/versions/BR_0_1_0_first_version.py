"""First version

Revision ID: BR-0-1-0
Revises: 
Create Date: 2020-11-24 10:50:31.822231

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'BR_0_1_0'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'refresher',
        sa.Column('id', sa.String, primary_key=True),
        sa.Column('hash', sa.String),
        sa.Column('url', sa.String),
        sa.Column('new', sa.Boolean, unique=False, default=True),  # Marks whether a dataset is brand new
        sa.Column('modified', sa.Boolean, unique=False, default=False),  # Marks whether a dataset is old but modified
        sa.Column('stale', sa.Boolean, unique=False, default=False),  # Marks whether a dataset is scheduled for deletion
        sa.Column('error', sa.Boolean, unique=False, default=False)
    )

def downgrade():
    op.drop_table('refresher')

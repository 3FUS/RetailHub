from sqlalchemy import Column, String, Date, Integer, DECIMAL, TIMESTAMP, func, Boolean, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DailySalesStoreModel(Base):
    __tablename__ = 'daily_sales_store'

    store_code = Column(String(30), primary_key=True, nullable=False)
    store_name = Column(String(255), nullable=True)
    trans_date = Column(Date, primary_key=True, nullable=False)
    finance_date = Column(Date, nullable=True)
    week_number = Column(Integer, nullable=True)
    trans_count = Column(Integer, nullable=False)
    return_trans_count = Column(Integer, nullable=True)
    customer_count = Column(Integer, nullable=True, default=0)
    retail_amt = Column(DECIMAL(17, 6), nullable=True)
    unit_amt = Column(DECIMAL(17, 6), nullable=True)
    quantity = Column(Integer, nullable=False, default=0)
    discount_amt = Column(DECIMAL(17, 6), nullable=True)
    sales_amt = Column(DECIMAL(17, 6), nullable=False)
    vat_amt = Column(DECIMAL(17, 6), nullable=True)
    net_amt = Column(DECIMAL(17, 6), nullable=True, default=0.000000)
    net_amt_eur = Column(DECIMAL(17, 6), nullable=True, default=0.000000)
    return_quantity = Column(Integer, nullable=False, default=0)
    return_amt = Column(DECIMAL(17, 6), nullable=False, default=0.000000)
    currency = Column(String(10), nullable=True)
    eur_rate = Column(DECIMAL(18, 4), nullable=True)
    traffic_qty = Column(Integer, nullable=True, default=0)
    create_time = Column(TIMESTAMP, nullable=True, default=func.current_timestamp())
    LA_A = Column(Integer, nullable=True)
    LA_B = Column(Integer, nullable=True)
    LA_C = Column(Integer, nullable=True)
    LA_D = Column(Integer, nullable=True)
    LA_E = Column(Integer, nullable=True)
    LA_F = Column(Integer, nullable=True)


class ECSalesModel(Base):
    __tablename__ = "ec_sales"

    order_id = Column(String(60), primary_key=True)  # 订单号
    is_return = Column(Boolean, primary_key=True)  # 是否退货
    recipient_name = Column(String(60))  # 收件人姓名
    order_source = Column(String(30))  # 订单来源
    payment_method = Column(String(30))  # 支付方式
    order_status = Column(String(30))  # 订单状态
    quantity = Column(Integer, primary_key=True)  # 数量
    product_code = Column(String(30))  # 商品编码
    product_sku = Column(String(30), primary_key=True)  # 商品SKU编码
    product_name = Column(String(80))  # 商品名称
    product_size = Column(String(30))  # 商品尺码
    total_amount = Column(Float)  # 订单总金额
    staff_code = Column(String(30), primary_key=True)  # 员工ID
    store_code = Column(String(30), primary_key=True)  # 门店ID
    area = Column(String(30))  # Area
    payment_time = Column(DateTime)  # 付款时间
    shipping_time = Column(DateTime)  # 发货时间/退货时间
    week = Column(String(30))  # Week
    is_wechat = Column(Boolean)  # 是否企微
    created_at = Column(TIMESTAMP, nullable=True, default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, nullable=True, default=func.current_timestamp())

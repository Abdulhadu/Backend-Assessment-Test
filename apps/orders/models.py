import uuid
from django.db import models
from django.core.validators import MinValueValidator
from decimal import Decimal


class Order(models.Model):
    ORDER_STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('confirmed', 'Confirmed'),
        ('processing', 'Processing'),
        ('shipped', 'Shipped'),
        ('delivered', 'Delivered'),
        ('cancelled', 'Cancelled'),
        ('refunded', 'Refunded'),
    ]
    CURRENCY_CHOICES = [
        ('USD', 'US Dollar'),
        ('EUR', 'Euro'),
        ('GBP', 'British Pound'),
        ('CAD', 'Canadian Dollar'),
    ]

    order_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(
        'tenants.Tenant',
        on_delete=models.CASCADE,
        db_column='tenant_id',
        related_name='orders'
    )
    external_order_id = models.CharField(max_length=100, help_text="External system order identifier")
    customer = models.UUIDField(null=True, blank=True, help_text="Customer reference (no FK constraint)")
    customer_name_snapshot = models.CharField(max_length=255)
    customer_email_snapshot = models.EmailField(max_length=255)
    total_amount = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        validators=[MinValueValidator(Decimal('0.00'))]
    )
    currency = models.CharField(max_length=3, choices=CURRENCY_CHOICES, default='USD')
    order_status = models.CharField(max_length=100, choices=ORDER_STATUS_CHOICES, default='pending')
    order_date = models.DateTimeField()
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'orders'
        indexes = [
            models.Index(fields=['tenant', 'order_date', 'order_status'], name='orders_tenant_date_status_idx'),
            models.Index(fields=['tenant', 'external_order_id'], name='orders_tenant_external_idx'),
            models.Index(fields=['customer'], name='orders_customer_idx'),
        ]
        constraints = [
            models.UniqueConstraint(fields=['tenant', 'external_order_id'], name='unique_tenant_external_order'),
        ]

    def __str__(self):
        return f"Order {self.external_order_id} ({self.order_status})"

    @property
    def customer_obj(self):
        if not self.customer:
            return None
        from apps.customers.models import Customer
        return Customer.objects.filter(customer_id=self.customer).first()


class OrderItem(models.Model):
    order_item_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    order = models.UUIDField(help_text="Order reference (no FK constraint)")
    tenant = models.ForeignKey(
        'tenants.Tenant',
        on_delete=models.CASCADE,
        db_column='tenant_id',
        related_name='order_items'
    )
    product = models.UUIDField(help_text="Product reference (no FK constraint)")
    quantity = models.PositiveIntegerField(validators=[MinValueValidator(1)])
    unit_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        validators=[MinValueValidator(Decimal('0.01'))]
    )
    line_total = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        validators=[MinValueValidator(Decimal('0.01'))]
    )

    class Meta:
        db_table = 'order_items'
        indexes = [
            models.Index(fields=['tenant'], name='order_items_tenant_idx'),
            models.Index(fields=['product'], name='order_items_product_idx'),
            models.Index(fields=['order'], name='order_items_order_idx'),
        ]

    def __str__(self):
        return f"Order Item {self.order_item_id}"

    @property
    def order_obj(self):
        from .models import Order
        return Order.objects.filter(order_id=self.order_ref).first()

    @property
    def product_obj(self):
        from apps.products.models import Product
        return Product.objects.filter(product_id=self.product).first()

    def save(self, *args, **kwargs):
        if not self.line_total:
            self.line_total = self.quantity * self.unit_price
        super().save(*args, **kwargs)

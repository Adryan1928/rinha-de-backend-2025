from pydantic import BaseModel
from uuid import UUID

class PaymentSchema(BaseModel):
    correlationId: UUID
    amount: float

class ProcessorSummarySchema(BaseModel):
    totalRequests: int = 0
    totalAmount: float = 0.0
from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from decimal import Decimal
from datetime import datetime
from typing import Optional


class InvoiceSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invoice_id:   str
    vendor_id:    str
    amount:       Decimal
    currency:     str
    invoice_date: datetime
    description:  str
    po_number:    Optional[str] = None


class MSASchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    msa_id:       str
    vendor_id:    str
    rate_ceiling: Decimal
    start_date:   datetime
    end_date:     datetime
    currency:     str

    @field_validator("rate_ceiling")
    @classmethod
    def ceiling_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError(
                f"rate_ceiling must be > 0 (got {v}). "
                "Use a large sentinel like 9_999_999 if no ceiling applies."
            )
        return v

    @model_validator(mode="after")
    def dates_must_be_ordered(self) -> "MSASchema":
        if self.start_date >= self.end_date:
            raise ValueError(
                f"MSA start_date ({self.start_date.date()}) must be "
                f"strictly before end_date ({self.end_date.date()})"
            )
        return self
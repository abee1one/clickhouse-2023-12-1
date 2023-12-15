from infi.clickhouse_orm import Model, DateTimeField, UInt8Field, Float32Field, Buffer


class CPUStats(Model):
    timestamp = DateTimeField()
    cpu_id = UInt8Field()
    cpu_persent = Float32Field()

    engine = Buffer()
"""
JSON Encoder customizado para tipos BSON do MongoDB
"""

import json
from datetime import datetime
from decimal import Decimal
from bson import ObjectId
from bson.decimal128 import Decimal128


class MongoJSONEncoder(json.JSONEncoder):
    """Encoder JSON customizado para tipos BSON do MongoDB"""
    
    def default(self, obj):
        if isinstance(obj, Decimal128):
            return float(obj.to_decimal())
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):  # Para outros tipos de data
            return obj.isoformat()
        elif hasattr(obj, 'to_decimal'):  # Para outros tipos Decimal
            return float(obj.to_decimal())
        
        return super().default(obj)


def json_response(data, status_code=200):
    """Helper para retornar resposta JSON com encoder customizado"""
    from flask import Response
    
    json_str = json.dumps(data, cls=MongoJSONEncoder, ensure_ascii=False)
    return Response(json_str, mimetype='application/json', status=status_code)
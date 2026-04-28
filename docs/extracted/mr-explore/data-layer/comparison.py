"""
Comparison engine for cross-hospital and cross-payer analysis.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from .database import MultiHospitalDatabase


@dataclass
class PayerComparisonRow:
    """A row in the payer comparison matrix."""
    code: str
    code_type: str
    description: str
    hospital_name: str
    payer_rates: Dict[str, float]  # payer_name -> negotiated_dollar
    gross_charge: Optional[float]
    min_rate: Optional[float]
    max_rate: Optional[float]
    price_range: Optional[float]


@dataclass
class HospitalComparisonRow:
    """A row in the hospital comparison matrix."""
    code: str
    code_type: str
    description: str
    payer_name: str
    hospital_rates: Dict[str, float]  # hospital_name -> avg_negotiated
    min_rate: Optional[float]
    max_rate: Optional[float]
    price_range: Optional[float]


class ComparisonEngine:
    """
    Engine for comparing charges across payers and hospitals.
    """
    
    def __init__(self, db: MultiHospitalDatabase):
        self.db = db
    
    def build_payer_matrix(self, 
                           code: str,
                           hospital_ids: Optional[List[int]] = None) -> List[PayerComparisonRow]:
        """
        Build a matrix comparing payer rates for a specific code.
        
        Args:
            code: CPT/CDM code to compare
            hospital_ids: Optional list of hospital IDs to include
            
        Returns:
            List of PayerComparisonRow objects
        """
        raw_data = self.db.compare_payers_for_code(code, hospital_ids)
        
        if not raw_data:
            return []
        
        # Group by hospital + description
        grouped = {}
        for row in raw_data:
            key = (row['hospital_name'], row['code_1'], row['description'])
            if key not in grouped:
                grouped[key] = {
                    'code': row['code_1'],
                    'code_type': row['code_1_type'],
                    'description': row['description'],
                    'hospital_name': row['hospital_name'],
                    'gross_charge': row['gross_charge'],
                    'payer_rates': {}
                }
            
            payer = row['payer_name']
            rate = row['negotiated_dollar']
            if payer and rate is not None:
                grouped[key]['payer_rates'][payer] = rate
        
        # Convert to PayerComparisonRow objects
        result = []
        for data in grouped.values():
            rates = list(data['payer_rates'].values())
            min_rate = min(rates) if rates else None
            max_rate = max(rates) if rates else None
            price_range = (max_rate - min_rate) if (min_rate and max_rate) else None

            result.append(PayerComparisonRow(
                code=data['code'],
                code_type=data['code_type'] or "",
                description=data['description'] or "",
                hospital_name=data['hospital_name'],
                payer_rates=data['payer_rates'],
                gross_charge=data['gross_charge'],
                min_rate=min_rate,
                max_rate=max_rate,
                price_range=price_range
            ))

        # Sort by price range (highest first)
        result.sort(key=lambda x: x.price_range or 0, reverse=True)
        return result
    
    def build_hospital_matrix(self,
                              code: str,
                              payer: Optional[str] = None) -> List[HospitalComparisonRow]:
        """
        Build a matrix comparing hospital rates for a specific code.
        
        Args:
            code: CPT/CDM code to compare
            payer: Optional payer to filter by
            
        Returns:
            List of HospitalComparisonRow objects
        """
        raw_data = self.db.compare_hospitals_for_code(code, payer)
        
        if not raw_data:
            return []
        
        # Group by payer
        grouped = {}
        for row in raw_data:
            payer_name = row['payer_name'] or "Unknown"
            if payer_name not in grouped:
                grouped[payer_name] = {
                    'code': row['code_1'],
                    'description': row['description'],
                    'payer_name': payer_name,
                    'hospital_rates': {}
                }
            
            hospital = row['hospital_name']
            avg_rate = row['avg_negotiated']
            if hospital and avg_rate is not None:
                grouped[payer_name]['hospital_rates'][hospital] = avg_rate
        
        # Convert to HospitalComparisonRow objects
        result = []
        for data in grouped.values():
            rates = list(data['hospital_rates'].values())
            min_rate = min(rates) if rates else None
            max_rate = max(rates) if rates else None
            price_range = (max_rate - min_rate) if (min_rate and max_rate) else None

            result.append(HospitalComparisonRow(
                code=data['code'],
                code_type="",
                description=data['description'] or "",
                payer_name=data['payer_name'],
                hospital_rates=data['hospital_rates'],
                min_rate=min_rate,
                max_rate=max_rate,
                price_range=price_range
            ))

        result.sort(key=lambda x: x.price_range or 0, reverse=True)
        return result
    
    def get_high_variance_services(self,
                                   hospital_ids: Optional[List[int]] = None,
                                   limit: int = 100) -> List[Dict[str, Any]]:
        """
        Find services with the highest price variance across payers.
        Useful for identifying negotiation opportunities.
        """
        return self.db.get_payer_variance(hospital_ids, limit=limit)
    
    def get_all_payers_for_comparison(self, hospital_ids: Optional[List[int]] = None) -> List[str]:
        """Get list of payers that appear across selected hospitals."""
        return self.db.get_unique_payers(hospital_ids)

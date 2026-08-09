"""
Configuration for the Olist-style E-Commerce Faker Data Generator.
Localized for Vietnam.

Includes: record counts, weighted distributions, distribution functions,
and Vietnamese master data (provinces, cities, product categories).
"""

import random
import numpy as np

# ---------------------------------------------------------------------------
# Random seed for reproducibility
# ---------------------------------------------------------------------------
RANDOM_SEED = 42

# ---------------------------------------------------------------------------
# Default record counts per entity
# ---------------------------------------------------------------------------
RECORD_COUNTS = {
    "customers": 500,
    "products": 100,
    "sellers": 50,
    "orders": 3000,
    "order_items": 8000,
    "payments": 3000,
    "reviews": 2000,
    "shipments": 3000,
}

# ---------------------------------------------------------------------------
# Weighted distributions
# ---------------------------------------------------------------------------

ORDER_STATUS_OPTIONS = ["delivered", "shipped", "processing", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.70, 0.10, 0.10, 0.05, 0.05]

PAYMENT_TYPE_OPTIONS = ["credit_card", "bank_transfer", "cod", "e_wallet"]
PAYMENT_TYPE_WEIGHTS = [0.25, 0.20, 0.30, 0.25]

SHIPPING_STATUS_OPTIONS = ["pending", "shipped", "in_transit", "delivered"]
# Transition order for CDC: pending -> shipped -> in_transit -> delivered
SHIPPING_STATUS_TRANSITION = {
    "pending": "shipped",
    "shipped": "in_transit",
    "in_transit": "delivered",
    "delivered": None,  # terminal state
}

# Ratio of CDC updates vs new shipments in generate_shipments
CDC_UPDATE_RATIO = 0.30  # ~30% of batch will be status updates on existing shipments

# ---------------------------------------------------------------------------
# Vietnamese master data
# ---------------------------------------------------------------------------

VIETNAM_PROVINCES = [
    {"province_code": "HN",  "province_name": "Ha Noi"},
    {"province_code": "HCM", "province_name": "Ho Chi Minh"},
    {"province_code": "DN",  "province_name": "Da Nang"},
    {"province_code": "HP",  "province_name": "Hai Phong"},
    {"province_code": "CT",  "province_name": "Can Tho"},
    {"province_code": "BD",  "province_name": "Binh Duong"},
    {"province_code": "DNG", "province_name": "Dong Nai"},
    {"province_code": "KH",  "province_name": "Khanh Hoa"},
    {"province_code": "TTH", "province_name": "Thua Thien Hue"},
    {"province_code": "QN",  "province_name": "Quang Ninh"},
    {"province_code": "NA",  "province_name": "Nghe An"},
    {"province_code": "TH",  "province_name": "Thanh Hoa"},
    {"province_code": "LA",  "province_name": "Long An"},
    {"province_code": "GL",  "province_name": "Gia Lai"},
    {"province_code": "DL",  "province_name": "Dak Lak"},
]

# Districts/cities grouped by province_code
VIETNAM_CITIES = {
    "HN":  ["Ba Dinh", "Hoan Kiem", "Cau Giay", "Dong Da", "Thanh Xuan", "Ha Dong", "Long Bien", "Nam Tu Liem"],
    "HCM": ["Quan 1", "Quan 3", "Quan 7", "Binh Thanh", "Go Vap", "Thu Duc", "Phu Nhuan", "Tan Binh"],
    "DN":  ["Hai Chau", "Thanh Khe", "Son Tra", "Lien Chieu", "Ngu Hanh Son"],
    "HP":  ["Hong Bang", "Le Chan", "Ngo Quyen", "Kien An"],
    "CT":  ["Ninh Kieu", "Binh Thuy", "Cai Rang", "O Mon"],
    "BD":  ["Thu Dau Mot", "Di An", "Thuan An", "Ben Cat"],
    "DNG": ["Bien Hoa", "Long Khanh", "Nhon Trach", "Vinh Cuu"],
    "KH":  ["Nha Trang", "Cam Ranh", "Ninh Hoa"],
    "TTH": ["Hue", "Huong Thuy", "Huong Tra"],
    "QN":  ["Ha Long", "Cam Pha", "Uong Bi", "Mong Cai"],
    "NA":  ["Vinh", "Cua Lo", "Thai Hoa"],
    "TH":  ["Thanh Hoa", "Bim Son", "Sam Son"],
    "LA":  ["Tan An", "Kien Tuong", "Ben Luc"],
    "GL":  ["Pleiku", "An Khe", "Ayun Pa"],
    "DL":  ["Buon Ma Thuot", "Buon Ho", "Ea Kar"],
}

# Province weights — HCM and HN dominate e-commerce
VIETNAM_PROVINCE_WEIGHTS = [
    0.25,  # HN
    0.30,  # HCM
    0.08,  # DN
    0.06,  # HP
    0.04,  # CT
    0.05,  # BD
    0.05,  # DNG
    0.03,  # KH
    0.02,  # TTH
    0.02,  # QN
    0.02,  # NA
    0.02,  # TH
    0.02,  # LA
    0.02,  # GL
    0.02,  # DL
]

PRODUCT_CATEGORIES = [
    "dien_thoai_phu_kien",      # phones & accessories
    "may_tinh_laptop",          # computers & laptops
    "dien_tu_dien_lanh",        # electronics & appliances
    "thoi_trang_nam",           # men's fashion
    "thoi_trang_nu",            # women's fashion
    "me_va_be",                 # mom & baby
    "suc_khoe_lam_dep",         # health & beauty
    "do_gia_dung",              # home & living
    "the_thao_da_ngoai",        # sports & outdoor
    "sach_van_phong_pham",      # books & stationery
    "thuc_pham_do_uong",        # food & beverages
    "o_to_xe_may",              # automotive & motorbike
    "giay_dep_tui_xach",        # shoes & bags
    "dong_ho_trang_suc",        # watches & jewelry
    "do_choi",                  # toys
    "may_anh_quay_phim",        # cameras
    "nha_cua_doi_song",         # home & life
    "bach_hoa_online",          # online groceries
    "voucher_dich_vu",          # vouchers & services
    "thiet_bi_so",              # digital devices
]

# Category weights — some categories sell more than others
PRODUCT_CATEGORY_WEIGHTS = [
    0.14, 0.10, 0.09, 0.08, 0.08, 0.07, 0.07, 0.06,
    0.05, 0.04, 0.04, 0.03, 0.03, 0.02, 0.02, 0.02,
    0.02, 0.01, 0.01, 0.02,
]

CARRIERS = [
    "giao_hang_nhanh",    # GHN
    "giao_hang_tiet_kiem", # GHTK
    "viettel_post",
    "j_and_t_express",
    "ninja_van",
    "best_express",
]

# Vietnamese product name prefixes (for generating realistic product names)
PRODUCT_NAME_PREFIXES = {
    "dien_thoai_phu_kien": ["Op lung", "Sac du phong", "Tai nghe", "Cap sac", "Kinh cuong luc"],
    "may_tinh_laptop": ["Chuot khong day", "Ban phim co", "USB", "Webcam", "Laptop stand"],
    "dien_tu_dien_lanh": ["Quat mini", "Noi chien khong dau", "May xay sinh to", "Am sieu toc", "Robot hut bui"],
    "thoi_trang_nam": ["Ao thun nam", "Quan jean nam", "Ao so mi", "Giay the thao", "That lung da"],
    "thoi_trang_nu": ["Dam nu", "Ao khoac nu", "Chan vay", "Tui xach nu", "Ao croptop"],
    "me_va_be": ["Binh sua", "Ta dan", "Xe day", "Do choi giao duc", "Quan ao tre em"],
    "suc_khoe_lam_dep": ["Kem chong nang", "Son moi", "Sua rua mat", "Vitamin", "Tinh dau"],
    "do_gia_dung": ["Khoan cam tay", "Bo dung cu", "Den LED", "Ong nuoc", "Ke sach"],
}


# ---------------------------------------------------------------------------
# Distribution functions
# ---------------------------------------------------------------------------

def weighted_choice(options: list, weights: list):
    """Pick one item from options using the given probability weights."""
    return np.random.choice(options, p=weights)


def log_normal_price(mean: float = 3.5, sigma: float = 1.0, min_val: float = 10000.0, max_val: float = 50000000.0) -> float:
    """
    Generate a price following a log-normal distribution (in VND).
    Most prices cluster low-to-mid range, few are very high.
    Returns a float rounded to nearest 1000 VND.
    """
    price = np.random.lognormal(mean=mean, sigma=sigma)
    # Scale to VND range
    price = price * 10000  # base scaling
    price = np.clip(price, min_val, max_val)
    return round(float(price) / 1000) * 1000  # round to nearest 1000 VND


def skewed_score() -> int:
    """
    Generate a review score (1-5) skewed toward higher scores.
    Distribution: 1->5%, 2->8%, 3->15%, 4->30%, 5->42%
    """
    return int(np.random.choice([1, 2, 3, 4, 5], p=[0.05, 0.08, 0.15, 0.30, 0.42]))


def random_installments() -> int:
    """Generate number of payment installments (1-12), weighted toward fewer."""
    return int(np.random.choice(
        [1, 2, 3, 4, 5, 6, 8, 10, 12],
        p=[0.35, 0.15, 0.12, 0.10, 0.08, 0.07, 0.06, 0.04, 0.03]
    ))


def pick_vietnam_location() -> dict:
    """Pick a random Vietnamese province + district, weighted toward HCM/HN."""
    province = VIETNAM_PROVINCES[np.random.choice(len(VIETNAM_PROVINCES), p=VIETNAM_PROVINCE_WEIGHTS)]
    city = random.choice(VIETNAM_CITIES[province["province_code"]])
    zip_code = f"{random.randint(100000, 999999)}"
    return {
        "city": city,
        "state": province["province_code"],
        "zip_code": zip_code,
    }


def pick_product_category() -> str:
    """Pick a product category using weighted distribution."""
    return str(np.random.choice(PRODUCT_CATEGORIES, p=PRODUCT_CATEGORY_WEIGHTS))


def generate_product_name(category: str) -> str:
    """Generate a realistic Vietnamese product name based on category."""
    prefixes = PRODUCT_NAME_PREFIXES.get(category)
    if prefixes:
        return f"{random.choice(prefixes)} - Mau {random.randint(1, 50)}"
    return f"San pham {category[:10]} #{random.randint(100, 999)}"

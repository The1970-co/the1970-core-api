export const DEFAULT_NAVIGATION_CONFIG = {
  version: 1,
  items: [
    {
      id: "tshirts",
      enabled: true,
      type: "MEGA",
      labelVi: "Áo phông",
      labelEn: "T-Shirts",
      link: "/collections/t-shirts",
      columns: [
        {
          id: "tshirts-main",
          titleVi: "ÁO PHÔNG",
          titleEn: "T-SHIRTS",
          links: [
            { id: "all-tshirts", labelVi: "Tất cả áo phông", labelEn: "All T-Shirts", link: "/collections/t-shirts", enabled: true },
            { id: "plain-tees", labelVi: "Áo phông trơn", labelEn: "Plain Tees", link: "/collections/t-shirts", enabled: true },
            { id: "heritage-tees", labelVi: "Áo phông Heritage", labelEn: "Heritage Tees", link: "/collections/t-shirts", enabled: true },
            { id: "athleisure-tees", labelVi: "Áo phông Athleisure", labelEn: "Athleisure Tees", link: "/collections/t-shirts", enabled: true }
          ]
        },
        {
          id: "tshirts-color",
          titleVi: "THEO MÀU",
          titleEn: "COLOR STORY",
          links: [
            { id: "olive", labelVi: "Olive Ash", labelEn: "Olive Ash", link: "/collections/all", enabled: true },
            { id: "sage", labelVi: "Heritage Sage", labelEn: "Heritage Sage", link: "/collections/all", enabled: true },
            { id: "tobacco", labelVi: "Tobacco", labelEn: "Tobacco", link: "/collections/all", enabled: true },
            { id: "navy", labelVi: "Vintage Navy", labelEn: "Vintage Navy", link: "/collections/all", enabled: true }
          ]
        }
      ],
      promo: {
        enabled: true,
        imageUrl: "https://cdn0199.cdn4s.com/media/3e12438546111e95cdfb901fd723bd8b.jpg",
        titleVi: "Xem áo phông",
        titleEn: "Shop T-Shirts",
        link: "/collections/t-shirts"
      }
    },
    {
      id: "shirts",
      enabled: true,
      type: "MEGA",
      labelVi: "Sơ mi",
      labelEn: "Shirts",
      link: "/collections/shirts",
      columns: [
        {
          id: "shirts-main",
          titleVi: "SƠ MI",
          titleEn: "SHIRTS",
          links: [
            { id: "all-shirts", labelVi: "Tất cả sơ mi", labelEn: "All Shirts", link: "/collections/shirts", enabled: true },
            { id: "checked-shirts", labelVi: "Sơ mi kẻ", labelEn: "Checked Shirts", link: "/collections/shirts", enabled: true },
            { id: "solid-shirts", labelVi: "Sơ mi trơn", labelEn: "Solid Shirts", link: "/collections/shirts", enabled: true },
            { id: "flannel-shirts", labelVi: "Sơ mi Flannel", labelEn: "Flannel Shirts", link: "/collections/shirts", enabled: true }
          ]
        },
        {
          id: "shirts-highlight",
          titleVi: "NỔI BẬT",
          titleEn: "HIGHLIGHTS",
          links: [
            { id: "shirts-new", labelVi: "Hàng mới", labelEn: "New Arrivals", link: "/collections/all", enabled: true },
            { id: "shirts-best", labelVi: "Bán chạy", labelEn: "Best Seller", link: "/collections/all", enabled: true },
            { id: "shirts-seasonal", labelVi: "Theo mùa", labelEn: "Seasonal Collection", link: "/collections/all", enabled: true }
          ]
        }
      ],
      promo: {
        enabled: true,
        imageUrl: "https://cdn0199.cdn4s.com/media/861%20v%C3%A0ng%208.jpg",
        titleVi: "Xem sơ mi",
        titleEn: "Explore Shirts",
        link: "/collections/shirts"
      }
    },
    {
      id: "bottoms",
      enabled: true,
      type: "MEGA",
      labelVi: "Quần",
      labelEn: "Bottoms",
      link: "/collections/pants",
      columns: [
        {
          id: "bottoms-main",
          titleVi: "QUẦN",
          titleEn: "BOTTOMS",
          links: [
            { id: "all-bottoms", labelVi: "Tất cả quần", labelEn: "All Bottoms", link: "/collections/pants", enabled: true },
            { id: "chinos", labelVi: "Quần kaki", labelEn: "Chinos", link: "/collections/pants", enabled: true },
            { id: "denim", labelVi: "Quần jean", labelEn: "Denim", link: "/collections/pants", enabled: true },
            { id: "shorts", labelVi: "Quần short", labelEn: "Shorts", link: "/collections/pants", enabled: true }
          ]
        },
        {
          id: "bottoms-edit",
          titleVi: "GỢI Ý",
          titleEn: "SHOP BY EDIT",
          links: [
            { id: "bottoms-best", labelVi: "Bán chạy", labelEn: "Best Seller", link: "/collections/all", enabled: true },
            { id: "bottoms-new", labelVi: "Hàng mới", labelEn: "New Arrivals", link: "/collections/all", enabled: true },
            { id: "bottoms-seasonal", labelVi: "Theo mùa", labelEn: "Seasonal Edit", link: "/collections/all", enabled: true }
          ]
        }
      ],
      promo: {
        enabled: true,
        imageUrl: "https://cdn0199.cdn4s.com/media/861%20%C4%91en%203.jpg",
        titleVi: "Xem quần",
        titleEn: "Shop Bottoms",
        link: "/collections/pants"
      }
    },
    {
      id: "new-arrivals",
      enabled: true,
      type: "LINK",
      labelVi: "Hàng mới",
      labelEn: "New Arrivals",
      link: "/collections/all",
      columns: [],
      promo: { enabled: false, imageUrl: "", titleVi: "", titleEn: "", link: "" }
    },
    {
      id: "collection",
      enabled: true,
      type: "LINK",
      labelVi: "Bộ sưu tập",
      labelEn: "Collection",
      link: "/collections/all",
      columns: [],
      promo: { enabled: false, imageUrl: "", titleVi: "", titleEn: "", link: "" }
    }
  ]
} as const;

export const DEFAULT_HOMEPAGE_CONFIG = {
  version: 1,
  blocks: [
    {
      id: "hero-main",
      type: "HERO_SPLIT",
      enabled: true,
      titleVi: "Hero đầu trang",
      titleEn: "Homepage hero",
      layout: "hero-split",
      settings: {},
      items: [
        {
          id: "hero-left",
          titleVi: "Tinh thần Mỹ cổ điển, được The 1970 hoàn thiện cho nhịp sống hằng ngày.",
          titleEn: "Vintage American mood, refined for The 1970.",
          subtitleVi: "THE 1970 — MÙA MỚI",
          subtitleEn: "THE 1970 — NEW SEASON",
          bodyVi: "Cổ điển, gọn gàng và dễ mặc.",
          bodyEn: "Classic aesthetic.",
          ctaVi: "Xem hàng mới",
          ctaEn: "Shop New Arrivals",
          link: "/collections/all",
          mediaMode: "single",
          autoplayMs: 4500,
          images: [{ id:"hero-1", url:"https://cdn0199.cdn4s.com/media/330ecc7e326b99ef7630ee3d87ffb925.jpg", altVi:"The 1970", altEn:"The 1970", link:"/collections/all" }]
        },
        {
          id: "hero-intro",
          titleVi: "Bớt chất workwear thô. Gọn gàng hơn cho phong cách vintage hằng ngày.",
          titleEn: "Less rugged costume. More sharp vintage everyday wear.",
          subtitleVi: "GÓC NHÌN THƯƠNG HIỆU",
          subtitleEn: "BRAND POINT OF VIEW",
          bodyVi: "The 1970 hướng tới cảm giác sạch hơn raw workwear, ấm hơn minimal basics và có bản sắc riêng.",
          bodyEn: "Cleaner than raw workwear, warmer than minimal basics, with a distinct identity of its own.",
          ctaVi: "",
          ctaEn: "",
          link: "/collections/all",
          mediaMode: "single",
          autoplayMs: 4500,
          images: [{ id:"intro-1", url:"https://cdn0199.cdn4s.com/media/330ecc7e326b99ef7630ee3d87ffb925.jpg", altVi:"Góc nhìn thương hiệu", altEn:"Brand point of view", link:"/collections/all" }]
        },
        {
          id: "olive",
          titleVi: "Olive Ash",
          titleEn: "Olive Ash",
          subtitleVi: "CÂU CHUYỆN MÀU SẮC",
          subtitleEn: "COLOR STORY",
          bodyVi: "",
          bodyEn: "",
          ctaVi: "",
          ctaEn: "",
          link: "/collections/all",
          mediaMode: "single",
          autoplayMs: 4500,
          images: [{ id:"olive-1", url:"https://cdn0199.cdn4s.com/media/3e12438546111e95cdfb901fd723bd8b.jpg", altVi:"Olive Ash", altEn:"Olive Ash", link:"/collections/all" }]
        },
        {
          id: "sage",
          titleVi: "Heritage Sage",
          titleEn: "Heritage Sage",
          subtitleVi: "CÂU CHUYỆN MÀU SẮC",
          subtitleEn: "COLOR STORY",
          bodyVi: "",
          bodyEn: "",
          ctaVi: "",
          ctaEn: "",
          link: "/collections/all",
          mediaMode: "single",
          autoplayMs: 4500,
          images: [{ id:"sage-1", url:"https://cdn0199.cdn4s.com/media/861%20v%C3%A0ng%208.jpg", altVi:"Heritage Sage", altEn:"Heritage Sage", link:"/collections/all" }]
        },
        {
          id: "tobacco",
          titleVi: "Tobacco",
          titleEn: "Tobacco",
          subtitleVi: "CÂU CHUYỆN MÀU SẮC",
          subtitleEn: "COLOR STORY",
          bodyVi: "",
          bodyEn: "",
          ctaVi: "",
          ctaEn: "",
          link: "/collections/all",
          mediaMode: "single",
          autoplayMs: 4500,
          images: [{ id:"tobacco-1", url:"https://cdn0199.cdn4s.com/media/861%20xmas%2090.jpg", altVi:"Tobacco", altEn:"Tobacco", link:"/collections/all" }]
        },
        {
          id: "navy",
          titleVi: "Vintage Navy",
          titleEn: "Vintage Navy",
          subtitleVi: "CÂU CHUYỆN MÀU SẮC",
          subtitleEn: "COLOR STORY",
          bodyVi: "",
          bodyEn: "",
          ctaVi: "",
          ctaEn: "",
          link: "/collections/all",
          mediaMode: "single",
          autoplayMs: 4500,
          images: [{ id:"navy-1", url:"https://cdn0199.cdn4s.com/media/861%20%C4%91en%203.jpg", altVi:"Vintage Navy", altEn:"Vintage Navy", link:"/collections/all" }]
        }
      ]
    },
    {
      id: "featured-three",
      type: "CARD_ROW",
      enabled: true,
      titleVi: "Ba thẻ nổi bật",
      titleEn: "Featured cards",
      layout: "cards-3",
      settings: {},
      items: [
        {
          id:"new-arrivals", titleVi:"Hàng mới về", titleEn:"New Arrivals", subtitleVi:"", subtitleEn:"", bodyVi:"", bodyEn:"", ctaVi:"", ctaEn:"",
          link:"/collections/all", mediaMode:"slider", autoplayMs:5000,
          images:[
            { id:"new-1", url:"https://cdn0199.cdn4s.com/media/3e12438546111e95cdfb901fd723bd8b.jpg", altVi:"Hàng mới", altEn:"New arrivals", link:"/collections/all" },
            { id:"new-2", url:"https://cdn0199.cdn4s.com/media/861%20v%C3%A0ng%208.jpg", altVi:"Hàng mới", altEn:"New arrivals", link:"/collections/all" }
          ]
        },
        {
          id:"best", titleVi:"Bán chạy", titleEn:"Best Seller", subtitleVi:"", subtitleEn:"", bodyVi:"", bodyEn:"", ctaVi:"", ctaEn:"",
          link:"/collections/all", mediaMode:"single", autoplayMs:5000,
          images:[{ id:"best-1", url:"https://cdn0199.cdn4s.com/media/861%20xmas%2090.jpg", altVi:"Bán chạy", altEn:"Best seller", link:"/collections/all" }]
        },
        {
          id:"seasonal", titleVi:"Bộ sưu tập theo mùa", titleEn:"Seasonal Collection", subtitleVi:"", subtitleEn:"", bodyVi:"", bodyEn:"", ctaVi:"", ctaEn:"",
          link:"/collections/all", mediaMode:"single", autoplayMs:5000,
          images:[{ id:"seasonal-1", url:"https://cdn0199.cdn4s.com/media/330ecc7e326b99ef7630ee3d87ffb925.jpg", altVi:"Theo mùa", altEn:"Seasonal collection", link:"/collections/all" }]
        }
      ]
    },
    {
      id:"story-wide", type:"STORY_WIDE", enabled:true, titleVi:"Bài editorial lớn", titleEn:"Editorial story", layout:"story-wide", settings:{},
      items:[{
        id:"story-wide-1", titleVi:"Những lớp trang phục theo mùa, sắc độ vintage và tinh thần Mỹ cho hằng ngày.",
        titleEn:"Seasonal layers, vintage tones, and everyday American mood.", subtitleVi:"THE 1970 JOURNAL", subtitleEn:"THE 1970 JOURNAL",
        bodyVi:"Khám phá câu chuyện hình ảnh và những sản phẩm nổi bật của mùa này.", bodyEn:"Explore the visual story and standout pieces from this season.",
        ctaVi:"Khám phá", ctaEn:"Explore Editorial", link:"/collections/all", mediaMode:"single", autoplayMs:5000,
        images:[{id:"story-wide-img",url:"https://cdn0199.cdn4s.com/media/330ecc7e326b99ef7630ee3d87ffb925.jpg",altVi:"The 1970 Journal",altEn:"The 1970 Journal",link:"/collections/all"}]
      }]
    },
    {
      id:"story-split", type:"STORY_SPLIT", enabled:true, titleVi:"Hai bài editorial", titleEn:"Split stories", layout:"story-split", settings:{},
      items:[
        {
          id:"split-new", titleVi:"Những thiết kế mới, hoàn thiện cho việc mặc hằng ngày.", titleEn:"Refined essentials, built for everyday wear.",
          subtitleVi:"HÀNG MỚI", subtitleEn:"NEW ARRIVALS", bodyVi:"", bodyEn:"", ctaVi:"Xem sản phẩm", ctaEn:"Shop New",
          link:"/collections/all", mediaMode:"single", autoplayMs:5000,
          images:[{id:"split-new-img",url:"https://cdn0199.cdn4s.com/media/861%20v%C3%A0ng%208.jpg",altVi:"Hàng mới",altEn:"New arrivals",link:"/collections/all"}]
        },
        {
          id:"split-best", titleVi:"Những thiết kế bền vững định hình phong cách The 1970.", titleEn:"Timeless pieces that define The 1970.",
          subtitleVi:"BÁN CHẠY", subtitleEn:"BEST SELLERS", bodyVi:"", bodyEn:"", ctaVi:"Xem sản phẩm", ctaEn:"Explore",
          link:"/collections/all", mediaMode:"single", autoplayMs:5000,
          images:[{id:"split-best-img",url:"https://cdn0199.cdn4s.com/media/861%20%C4%91en%203.jpg",altVi:"Bán chạy",altEn:"Best sellers",link:"/collections/all"}]
        }
      ]
    },
    {
      id:"products", type:"PRODUCT_GRID", enabled:true, titleVi:"Sản phẩm nổi bật", titleEn:"Featured Products",
      layout:"product-grid", settings:{limit:8, onlyFeatured:false}, items:[]
    }
  ]
} as const;

import type { NextSeoProps } from "next-seo";

export const SEO: NextSeoProps = {
  titleTemplate: "%s",
  defaultTitle: "Lineagentic - Data Lineage Visualization",
  description:
    "Lineagentic is a tool for analyzing relationships and lineage across diverse data processing scripts including python, sql, java, airflow, spark, etc.",
  canonical: "https://lineagentic.com",
  openGraph: {
    type: "website",
    locale: "en_US",
    url: "https://lineagentic.com",
    siteName: "Lineagentic",
    title: "Lineagentic - Data Lineage Visualization",
    description:
      "Lineagentic is a tool for analyzing relationships and lineage across diverse data processing scripts including python, sql, java, airflow, spark, etc.",
    images: [
      {
        url: "https://lineagentic.com/assets/192.png",
        width: 192,
        height: 192,
        alt: "Lineagentic Logo",
      },
    ],
  },
  twitter: {
    handle: "@lineagentic",
    site: "@lineagentic",
    cardType: "summary_large_image",
  },
  additionalLinkTags: [
    {
      rel: "icon",
      href: "/favicon.svg",
    },
    {
      rel: "apple-touch-icon",
      href: "/assets/192.png",
    },
    {
      rel: "manifest",
      href: "/manifest.json",
    },
  ],
  additionalMetaTags: [
    {
      name: "theme-color",
      content: "#8b5cf6",
    },
  ],
};

import {Swiper, SwiperSlide} from "swiper/react";
import {Pagination, Autoplay, Mousewheel} from "swiper/modules";
import "swiper/css";
import "swiper/css/pagination";
import "swiper/css/navigation";

import styles from "./Slider.module.css";
import Container from "../../layouts/Container/Container.jsx";

const slides = [
    {
        href: "https://autopartner.com/",
        title: "Autopartner Rzeszow",
        img: "img/main/slider/ap-slider.jpg",
        alt: "Autopartner Rzeszow site screenshot",
    },
    {
        href: "https://www.autopartner.pl/",
        title: "Autopartner Gdansk",
        img: "img/main/slider/gdansk-slider.jpg",
        alt: "Autopartner Gdansk site screenshot",
    },
    {
        href: "https://www.motorol.pl/",
        title: "Motorol",
        img: "img/main/slider/motorol-slider.jpg",
        alt: "Motorol site screenshot",
    },
];

export default function Slider() {
    return (
        <div className={styles.wrapper}>
            <Container>
                <Swiper
                    modules={[Pagination, Autoplay, Mousewheel]}
                    spaceBetween={20}
                    slidesPerView={1}
                    loop={true}
                    mousewheel={true}
                    autoplay={{delay: 4000, disableOnInteraction: false}}
                    pagination={{clickable: true, dynamicBullets: true}}
                    navigation
                    breakpoints={{
                        550: {slidesPerView: 2},
                        1024: {slidesPerView: 3},
                    }}
                    className={styles.swiper}
                >
                    {slides.map((slide, idx) => (
                        <SwiperSlide key={idx} className={styles.slide}>
                            <a
                                href={slide.href}
                                target="_blank"
                                rel="noopener noreferrer"
                                className={styles.slideLink}
                            >
                                <span className={styles.slideTitle}>{slide.title}</span>
                                <img src={slide.img} alt={slide.alt} className={styles.slideImg}/>
                            </a>
                        </SwiperSlide>
                    ))}
                </Swiper>
            </Container>
        </div>
    );
}

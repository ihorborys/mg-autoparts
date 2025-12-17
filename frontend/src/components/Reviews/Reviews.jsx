import styles from "./Reviews.module.css";
import Loader from "../Loader/Loader.jsx";
import ErrorMessage from "../ErrorMessage/ErrorMessage.jsx";
import {useSelector} from "react-redux";
import {
    selectCamper,
    selectError,
    selectLoading,
} from "../../redux/productsSlice.js";
// import BookForm from "../BookForm/BookForm.jsx";
import {nanoid} from "nanoid";

const Reviews = () => {
    const camper = useSelector(selectCamper);
    const loading = useSelector(selectLoading);
    const errorMessage = useSelector(selectError);

    return (
        <div className={styles.container}>
            <div className={styles.additionalInfoContainer}>
                <ul className={styles.reviewsList}>
                    {camper.reviews.map((review) => (
                        <li key={nanoid()} className={styles.reviewsItem}>
                            <ul className={styles.list}>
                                <li className={styles.item}>
                                    <div className={styles.icon}>
                    <span className={styles.iconText}>
                      {review.reviewer_name[0]}
                    </span>
                                    </div>
                                </li>
                                <div className={styles.item}>
                                    <span className={styles.name}>{review.reviewer_name}</span>
                                    <div className={styles.rating}>
                                        {Array.from({
                                            length: Math.floor(Number(review.reviewer_rating)),
                                        }).map(() => (
                                            <li key={nanoid()} className={styles.ratingIconItem}>
                                                <svg
                                                    className={styles.ratingIcon}
                                                    width="16"
                                                    height="16"
                                                >
                                                    <use href="/icons.svg#icon-rating-pressed"></use>
                                                </svg>
                                            </li>
                                        ))}
                                        {Array.from({
                                            length: 5 - Math.floor(Number(review.reviewer_rating)),
                                        }).map(() => (
                                            <svg
                                                key={nanoid()}
                                                className={styles.ratingIcon}
                                                width="16"
                                                height="16"
                                            >
                                                <use href="/icons.svg#icon-StarDefault_2"></use>
                                            </svg>
                                        ))}
                                    </div>
                                </div>
                            </ul>
                            <span className={styles.reviewsDescription}>
                {review.comment}
              </span>
                        </li>
                    ))}
                </ul>
            </div>

            <div className={styles.formContainer}>
                {/*<BookForm />*/}
            </div>
            {loading && <Loader/>}
            {errorMessage && <ErrorMessage errorMessage={errorMessage}/>}
        </div>
    );
};

export default Reviews;

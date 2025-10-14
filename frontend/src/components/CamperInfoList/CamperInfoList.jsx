import styles from "./CamperInfoList.module.css";
import FavoriteBtn from "../FavoriteBtn/FavoriteBtn.jsx";

const CamperInfoList = ({ camper }) => {
  return (
    <>
      <ul className={styles.namePriceFavoriteWrapper}>
        <h2 className={styles.name}>
          {camper.name.length < 26
            ? camper.name
            : camper.name.substring(0, 26) + "..."}
        </h2>
        <div className={styles.priceFavorite}>
          <li className={styles.price}>{`€${camper.price.toFixed(2)}`}</li>
          <FavoriteBtn camperId={camper.id} />
        </div>
      </ul>
      <ul className={styles.ratingLocationWrapper}>
        <svg className={styles.favorite} width={16} height={16}>
          <use href="./icons.svg#icon-rating-pressed"></use>
        </svg>
        <li className={styles.rating}>{camper.rating.toFixed(1)}</li>
        <li className={styles.reviews}>
          {`(${camper.reviews.length} Reviews)`}
        </li>
        <svg className={styles.map} width={16} height={16}>
          <use href="./icons.svg#icon-map_active"></use>
        </svg>
        <li className={styles.location}>{camper.location}</li>
      </ul>
      <ul className={styles.descriptionWrapper}>
        <li className={styles.description}>
          {camper.description.length < 61
            ? camper.description
            : camper.description.substring(0, 61) + "..."}
        </li>
      </ul>
    </>
  );
};

export default CamperInfoList;

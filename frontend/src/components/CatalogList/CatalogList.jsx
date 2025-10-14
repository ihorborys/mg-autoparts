import styles from "./CatalogList.module.css";
import { useSelector } from "react-redux";
import {
  selectCampers,
  selectError,
  selectLoading,
} from "../redux/campersSlice.js";
import Button from "../Button/Button.jsx";
import EquipmentList from "../EquipmentList/EquipmentList.jsx";
import CamperInfoList from "../CamperInfoList/CamperInfoList.jsx";
import { useNavigate } from "react-router-dom";
import LoadMoreBtn from "../LoadMoreBtn/LoadMoreBtn.jsx";
import Loader from "../Loader/Loader.jsx";
import ErrorMessage from "../ErrorMessage/ErrorMessage.jsx";
import { nanoid } from "nanoid";

const CatalogList = () => {
  const campers = useSelector(selectCampers);
  const navigate = useNavigate();

  const loading = useSelector(selectLoading);
  const errorMessage = useSelector(selectError);

  const handleClick = (id) => {
    navigate(`/catalog/${id}`);
  };

  console.log(campers);
  return (
    <ul className={styles.list}>
      {campers &&
        campers.map((camper) => (
          <li key={nanoid()} className={styles.item}>
            <ul className={styles.camperInfoContainer}>
              <li className={styles.camperInfoImage}>
                <img
                  className={styles.image}
                  src={camper.gallery[0].thumb}
                  width={292}
                  height={320}
                  alt="Camper image"
                />
              </li>
              <li className={styles.camperInfoWrapper}>
                <CamperInfoList camper={camper} />
                <EquipmentList camper={camper} />
                <Button handleOnClick={() => handleClick(camper.id)}>
                  Show more
                </Button>
              </li>
            </ul>
          </li>
        ))}
      <div>
        {loading && <Loader />}
        {errorMessage && <ErrorMessage errorMessage={errorMessage} />}
        <LoadMoreBtn disabled={loading} />
      </div>
    </ul>
  );
};

export default CatalogList;

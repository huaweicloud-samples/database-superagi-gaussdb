import axios from 'axios';
import Cookies from "js-cookie";

const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID;
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8001';
const GOOGLE_ANALYTICS_MEASUREMENT_ID =  process.env.GOOGLE_ANALYTICS_MEASUREMENT_ID;
const GOOGLE_ANALYTICS_API_SECRET =  process.env.GOOGLE_ANALYTICS_API_SECRET;
const MIXPANEL_AUTH_ID = process.env.NEXT_PUBLIC_MIXPANEL_AUTH_ID

export const baseUrl = () => {
  return API_BASE_URL;
};

export const githubClientId = () => {
  return GITHUB_CLIENT_ID;
};

export const analyticsMeasurementId = () => {
  return GOOGLE_ANALYTICS_MEASUREMENT_ID;
};

export const analyticsApiSecret = () => {
  return GOOGLE_ANALYTICS_API_SECRET;
};

export const mixpanelId = () => {
  return MIXPANEL_AUTH_ID;
};

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    common: {
      'Content-Type': 'application/json',
    },
  },
});

api.interceptors.request.use(config => {
  if (typeof window !== 'undefined') {
    // const accessToken = localStorage.getItem("accessToken");
    const accessToken = Cookies.get("accessToken");
    if (accessToken) {
      config.headers['Authorization'] = `Bearer ${accessToken}`;
    }
  }
  return config;
});

// 过期/无效凭证处理：401 时清除本地凭证并整页重载一次。
// 本环境无凭证请求可用（DEV/本地部署），清除后启动链可正常走通，不会循环 401。
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (typeof window !== 'undefined' && error.response && error.response.status === 401) {
      if (Cookies.get("accessToken")) {
        Cookies.remove("accessToken");
        window.location.reload();
      }
    }
    return Promise.reject(error);
  }
);

export default api;

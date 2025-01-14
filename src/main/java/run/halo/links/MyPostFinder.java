package run.halo.links;

import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import run.halo.app.core.extension.content.Post;
import run.halo.app.extension.ListResult;
import run.halo.links.vo.MyListedPostVo;
import run.halo.links.vo.MyPostVo;
import java.util.List;
import java.util.Map;

/**
 * A finder for {@link Post}.
 *
 * @author guqing
 * @since 2.0.0
 */
public interface MyPostFinder {

    /**
     * Gets post detail by name.
     * <p>
     * We ensure the post is public, non-deleted and published.
     *
     * @param postName is post name
     * @return post detail
     */
    Mono<MyPostVo> getByName(String postName);


    Flux<MyListedPostVo> listAll();


    Map<String,String> getAnnotationsByArticle(String name, String patternString);

    Mono<ListResult<MyListedPostVo>> listByCategoryAndChildren(@Nullable Integer page, @Nullable Integer size,
        String categoryName);
}

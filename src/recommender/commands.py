import click

@click.group()
@click.option('--data-set', default='train', help='Type of the data set')
@click.pass_context
def cli(ctx, data_set):
    ctx.obj['data_set'] = data_set


@cli.command()
@click.option('--what', default='', type=click.Choice(['products', 'order_products', 'orders']), help='Load data from files')
@click.pass_context
def load(ctx, what):
    from loader import load_products, load_order_products, load_orders

    if what == 'products':
        load_products(ctx.obj['data_set'])

    if what == 'order_products':
        load_order_products(ctx.obj['data_set'])

    if what == 'orders':
        load_orders(ctx.obj['data_set'])


@cli.command()
@click.option('--samples', default=100, help='Number of samples')
@click.option('--what', default='', type=click.Choice(['products_by_user', 'products_totally', 'users_similarity', 'orders_similarity']), 
              help='Analyze data')
@click.option('--orders', default=1, help='Number of orders')
@click.option('--last-order-id', default=None, help='Last order id')
@click.option('--user-id', default=None, help='User id')
@click.pass_context
def analyze(ctx, samples, what, orders, last_order_id, user_id):
    from analyze import analyze_products_by_user, analyze_users_similarity, analyze_orders_similarity, \
                        analyze_products_totally, analyze_orders_similarity_multi, analyze_users_similarity_multi

    if what == 'products_by_user':
        analyze_products_by_user(data_set=ctx.obj['data_set'])
    
    if what == 'products_totally':
        analyze_products_totally(data_set=ctx.obj['data_set'])

    if what == 'users_similarity':
        # analyze_users_similarity(data_set=ctx.obj['data_set'], samples=ctx.obj['samples'])
        analyze_users_similarity_multi(data_set=ctx.obj['data_set'], samples=samples, user_id=user_id)

    if what == 'orders_similarity':
        # analyze_orders_similarity(data_set=ctx.obj['data_set'], samples=samples)
        analyze_orders_similarity_multi(data_set=ctx.obj['data_set'], samples=samples, orders=orders, last_order_id=last_order_id, user_id=user_id)


@cli.command()
@click.option('--what', default='', type=click.Choice(['most_frequently_bought', 'most_frequently_bought_by_user',
                                                       'most_frequently_bought_in_similar_orders', 'most_frequently_bought_by_similar_users']), 
              help='Recommend products')
@click.option('--user-id', default='', help='User id')
@click.pass_context
def recommend(ctx, what, user_id):
    from recommend import most_frequently_bought, most_frequently_bought_by_user, most_frequently_bought_in_similar_orders,\
                          most_frequently_bought_by_similar_users

    if what == 'most_frequently_bought':
        most_frequently_bought(data_set=ctx.obj['data_set'], user_id=user_id)
    
    if what == 'most_frequently_bought_by_user':
        most_frequently_bought_by_user(data_set=ctx.obj['data_set'], user_id=user_id)

    if what == 'most_frequently_bought_by_similar_users':
        most_frequently_bought_by_similar_users(data_set=ctx.obj['data_set'], user_id=user_id)

    if what == 'most_frequently_bought_in_similar_orders':
        most_frequently_bought_in_similar_orders(data_set=ctx.obj['data_set'], user_id=user_id)
